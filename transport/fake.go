package transport

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mmcloughlin/md4"
	"github.com/zeebo/xxh3"

	"sc/model"
)

// FakeBackend synthesizes an arbitrarily large directory tree with no disk or
// network behind it. Every attribute of every entry is derived from a hash of
// its path, so nothing is materialized until a directory is listed and a
// 10M-object tree costs no memory at rest. Two fake backends built with the
// same parameters produce byte-identical trees; the drop/diff knobs skew one
// side against the other. Intended for profiling the scanner, the tree model
// and the UI at scales that are painful to stage on real storage.
//
//	fake://huge                            preset shape
//	fake://x?dirs=8&files=25&depth=5        explicit shape (≈1M objects)
//	fake://x?latency=2ms                    per-List delay, simulates a network backend
//	fake://x?diff=0.1&drop=0.02             10% of files differ, 2% missing on this side
//
// Presets (host component): tiny, small, medium, large, huge, insane.
// Query parameters override the preset.
//
// Writes (copy, delete, rename, touch) land in an in-memory overlay on top of
// the generated tree, so the fake works as a copy destination. Limits: renaming
// a generated directory does not carry its generated subtree along, and the
// overlay is not persisted.
type FakeBackend struct {
	url       string
	seed      uint64
	dirs      int
	files     int
	depth     int
	maxSize   int64
	vary      bool
	drop      float64
	dropDirs  float64
	diff      float64
	latency   time.Duration
	ckLatency time.Duration
	noData    bool
	cksumAlgo string

	mu   sync.RWMutex
	over map[string]map[string]*fakeMut // dir → name → override or tombstone
	data map[string][]byte              // relPath → content written into the overlay
}

type fakeMut struct {
	entry   model.FileEntry
	deleted bool
}

// Distinct salts keep the per-attribute hashes of one path independent.
const (
	fakeSaltShape = 0x9e3779b97f4a7c15
	fakeSaltSize  = 0xbf58476d1ce4e5b9
	fakeSaltTime  = 0x94d049bb133111eb
	fakeSaltDrop  = 0xd6e8feb86659fd93
	fakeSaltDiff  = 0xa0761d6478bd642f
	fakeSaltData  = 0xe7037ed1a0b428db
)

var fakeEpoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

var fakePresets = map[string][3]int{
	// name: {dirs per dir, files per dir, depth}
	"tiny":   {3, 10, 2},  // ~140 objects
	"small":  {5, 20, 3},  // ~3.3k
	"medium": {6, 30, 4},  // ~48k
	"large":  {6, 25, 5},  // ~240k
	"huge":   {8, 25, 5},  // ~1M
	"insane": {10, 40, 5}, // ~4.5M
}

func NewFakeBackend(rawURL string) (*FakeBackend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("fake: %v", err)
	}
	shape, ok := fakePresets[u.Host]
	if !ok {
		shape = fakePresets["medium"]
	}
	b := &FakeBackend{
		url:     rawURL,
		seed:    1,
		dirs:    shape[0],
		files:   shape[1],
		depth:   shape[2],
		maxSize: 1 << 20,
		over:    make(map[string]map[string]*fakeMut),
		data:    make(map[string][]byte),
	}
	if err := b.applyQuery(u.Query()); err != nil {
		return nil, err
	}
	dirs, files, approx := b.estimate()
	tilde := ""
	if approx {
		tilde = ">"
	}
	Log.Add("fake", "<<<", fmt.Sprintf("%s: dirs=%d files=%d depth=%d seed=%d → %s%s dirs, %s%s files",
		rawURL, b.dirs, b.files, b.depth, b.seed, tilde, formatCount(dirs), tilde, formatCount(files)))
	return b, nil
}

func (b *FakeBackend) applyQuery(q url.Values) error {
	var errs []string
	num := func(key string, dst any) {
		raw := q.Get(key)
		if raw == "" {
			return
		}
		switch d := dst.(type) {
		case *int:
			v, err := strconv.Atoi(raw)
			if err != nil || v < 0 {
				errs = append(errs, key+"="+raw)
				return
			}
			*d = v
		case *int64:
			v, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || v < 0 {
				errs = append(errs, key+"="+raw)
				return
			}
			*d = v
		case *uint64:
			v, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				errs = append(errs, key+"="+raw)
				return
			}
			*d = v
		case *float64:
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil || v < 0 || v > 1 {
				errs = append(errs, key+"="+raw)
				return
			}
			*d = v
		case *time.Duration:
			v, err := time.ParseDuration(raw)
			if err != nil || v < 0 {
				errs = append(errs, key+"="+raw)
				return
			}
			*d = v
		case *bool:
			*d = raw != "0" && raw != "false"
		}
	}
	num("dirs", &b.dirs)
	num("files", &b.files)
	num("depth", &b.depth)
	num("seed", &b.seed)
	num("size", &b.maxSize)
	num("drop", &b.drop)
	num("dropdirs", &b.dropDirs)
	num("diff", &b.diff)
	num("latency", &b.latency)
	num("cklatency", &b.ckLatency)
	num("vary", &b.vary)
	num("nodata", &b.noData)
	if len(errs) > 0 {
		return fmt.Errorf("fake: bad parameter %s", strings.Join(errs, ", "))
	}
	if b.depth > 64 {
		b.depth = 64
	}
	if b.maxSize < 1 {
		b.maxSize = 1
	}
	return nil
}

// estimate returns the object counts the current shape implies. approx is set
// when the count was clamped to keep the multiplication from running away.
func (b *FakeBackend) estimate() (dirs, files int64, approx bool) {
	var level, total int64 = 1, 1
	for k := 1; k <= b.depth; k++ {
		level *= int64(b.dirs)
		total += level
		if total > 1e9 {
			approx = true
			break
		}
	}
	return total - 1, total * int64(b.files), approx
}

func formatCount(n int64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	default:
		return strconv.FormatInt(n, 10)
	}
}

func (b *FakeBackend) BasePath() string { return b.url }

func (b *FakeBackend) List(ctx context.Context, relDir string) ([]model.FileEntry, error) {
	if err := fakeDelay(ctx, b.latency); err != nil {
		return nil, err
	}
	if b.tombstoned(relDir) {
		return nil, fmt.Errorf("fake: %s: %w", relDir, fs.ErrNotExist)
	}
	return b.applyOverlay(relDir, b.genEntries(relDir)), nil
}

// genEntries derives one directory's children from the hash of its path. Dirs
// come first so a listing looks like a real one; both groups are name-sorted by
// construction.
func (b *FakeBackend) genEntries(relDir string) []model.FileEntry {
	depth := 0
	if relDir != "" {
		depth = strings.Count(relDir, "/") + 1
	}
	nd, nf := b.dirs, b.files
	if depth >= b.depth {
		nd = 0
	}
	if b.vary {
		h := xxh3.HashStringSeed(relDir, b.seed^fakeSaltShape)
		nd = varyCount(nd, h)
		nf = varyCount(nf, h>>32)
	}

	out := make([]model.FileEntry, 0, nd+nf)
	for i := 0; i < nd; i++ {
		name := fakeName("d", i, "")
		rel := joinRel(relDir, name)
		if b.dropDirs > 0 && fakeFrac(b.hash(rel, fakeSaltDrop)) < b.dropDirs {
			continue
		}
		e := model.FileEntry{RelPath: rel, Name: name, IsDir: true, Mode: os.ModeDir | 0755}
		b.fillTimes(&e, rel)
		out = append(out, e)
	}
	for i := 0; i < nf; i++ {
		name := fakeName("f", i, ".dat")
		rel := joinRel(relDir, name)
		if b.drop > 0 && fakeFrac(b.hash(rel, fakeSaltDrop)) < b.drop {
			continue
		}
		e := model.FileEntry{
			RelPath: rel,
			Name:    name,
			Size:    int64(b.hash(rel, fakeSaltSize) % uint64(b.maxSize)),
			Mode:    0644,
		}
		b.fillTimes(&e, rel)
		if b.diff > 0 && fakeFrac(b.hash(rel, fakeSaltDiff)) < b.diff {
			h := b.hash(rel, fakeSaltDiff)
			e.Size += int64(1 + h%4096)
			e.ModTime = e.ModTime.Add(time.Duration(1+h%3600) * time.Second)
		}
		out = append(out, e)
	}
	return out
}

func (b *FakeBackend) fillTimes(e *model.FileEntry, rel string) {
	h := b.hash(rel, fakeSaltTime)
	e.ModTime = fakeEpoch.Add(time.Duration(h%(5*365*24*3600)) * time.Second)
	e.ATime = e.ModTime.Add(time.Duration(h%86400) * time.Second)
	e.CTime = e.ModTime
	e.BirthTime = e.ModTime.Add(-time.Duration(h%(30*24*3600)) * time.Second)
}

func (b *FakeBackend) hash(relPath string, salt uint64) uint64 {
	return xxh3.HashStringSeed(relPath, b.seed^salt)
}

// varyCount spreads a count over [n/4, 7n/4] so the tree is lumpy the way real
// ones are — a uniform fan-out hides bugs that only bite on wide directories.
func varyCount(n int, h uint64) int {
	if n == 0 {
		return 0
	}
	return n/4 + int(h%uint64(3*n/2+1))
}

func fakeFrac(h uint64) float64 { return float64(h>>11) / float64(uint64(1)<<53) }

func fakeName(prefix string, i int, ext string) string {
	s := strconv.Itoa(i)
	var sb strings.Builder
	sb.Grow(len(prefix) + 6 + len(ext))
	sb.WriteString(prefix)
	for n := len(s); n < 5; n++ {
		sb.WriteByte('0')
	}
	sb.WriteString(s)
	sb.WriteString(ext)
	return sb.String()
}

func joinRel(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

func fakeDelay(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// applyOverlay folds writes over the generated listing. The common case — a
// directory nobody has written to — returns the generated slice untouched.
func (b *FakeBackend) applyOverlay(relDir string, entries []model.FileEntry) []model.FileEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()
	muts := b.over[relDir]
	if len(muts) == 0 {
		return entries
	}
	out := entries[:0]
	for _, e := range entries {
		m, ok := muts[e.Name]
		if !ok {
			out = append(out, e)
			continue
		}
		if m.deleted {
			continue
		}
		out = append(out, m.entry)
	}
	for name, m := range muts {
		if m.deleted || containsName(entries, name) {
			continue
		}
		out = append(out, m.entry)
	}
	return out
}

func containsName(entries []model.FileEntry, name string) bool {
	for i := range entries {
		if entries[i].Name == name {
			return true
		}
	}
	return false
}

// tombstoned reports whether relDir or any ancestor has been removed.
func (b *FakeBackend) tombstoned(relPath string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.over) == 0 {
		return false
	}
	for p := relPath; p != ""; p = model.DirOf(p) {
		name := p
		if i := strings.LastIndexByte(p, '/'); i >= 0 {
			name = p[i+1:]
		}
		if m := b.over[model.DirOf(p)][name]; m != nil && m.deleted {
			return true
		}
	}
	return false
}

func (b *FakeBackend) putMut(relPath string, m *fakeMut) {
	dir := model.DirOf(relPath)
	name := relPath[len(dir):]
	name = strings.TrimPrefix(name, "/")
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.over[dir] == nil {
		b.over[dir] = make(map[string]*fakeMut)
	}
	b.over[dir][name] = m
}

// lookup returns the current entry for relPath — the overlay's version if one
// exists, otherwise the generated one.
func (b *FakeBackend) lookup(relPath string) (model.FileEntry, bool) {
	if relPath == "" || b.tombstoned(relPath) {
		return model.FileEntry{}, false
	}
	dir := model.DirOf(relPath)
	name := strings.TrimPrefix(relPath[len(dir):], "/")
	b.mu.RLock()
	m := b.over[dir][name]
	b.mu.RUnlock()
	if m != nil {
		return m.entry, !m.deleted
	}
	for _, e := range b.genEntries(dir) {
		if e.Name == name {
			return e, true
		}
	}
	return model.FileEntry{}, false
}

func (b *FakeBackend) Checksum(ctx context.Context, relPath string) (string, error) {
	if err := fakeDelay(ctx, b.ckLatency); err != nil {
		return "", err
	}
	e, ok := b.lookup(relPath)
	if !ok {
		return "", fmt.Errorf("fake: %s: %w", relPath, fs.ErrNotExist)
	}
	var h hash.Hash
	switch b.cksumAlgo {
	case "xxh3":
		h = xxh3.New()
	case "sha1":
		h = sha1.New()
	case "md5":
		h = md5.New()
	case "md4":
		h = md4.New()
	default:
		h = sha256.New()
	}
	rd, err := b.openEntry(relPath, e, 0)
	if err != nil {
		return "", err
	}
	defer rd.Close()
	if _, err := io.Copy(h, rd); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (b *FakeBackend) ProbeChecksums() []string {
	return []string{"xxh3", "sha256", "sha1", "md5", "md4"}
}

func (b *FakeBackend) SetChecksumAlgo(algo string) { b.cksumAlgo = algo }

func (b *FakeBackend) Open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	return b.OpenAt(ctx, relPath, 0)
}

func (b *FakeBackend) OpenAt(ctx context.Context, relPath string, offset int64) (io.ReadCloser, error) {
	e, ok := b.lookup(relPath)
	if !ok {
		return nil, fmt.Errorf("fake: %s: %w", relPath, fs.ErrNotExist)
	}
	return b.openEntry(relPath, e, offset)
}

func (b *FakeBackend) openEntry(relPath string, e model.FileEntry, offset int64) (io.ReadCloser, error) {
	if e.IsDir {
		return nil, fmt.Errorf("fake: %s is a directory", relPath)
	}
	b.mu.RLock()
	data, stored := b.data[relPath]
	b.mu.RUnlock()
	if stored {
		if offset > int64(len(data)) {
			offset = int64(len(data))
		}
		return io.NopCloser(bytes.NewReader(data[offset:])), nil
	}
	if offset > e.Size {
		offset = e.Size
	}
	return &fakeReader{seed: b.hash(relPath, fakeSaltData), off: offset, size: e.Size}, nil
}

// fakeReader streams deterministic bytes for a synthetic file without ever
// holding the whole body: block N is splitmix64 seeded by (file seed, N).
type fakeReader struct {
	seed  uint64
	off   int64
	size  int64
	block [4096]byte
	have  int64 // block index currently in block[], -1 when empty
	first bool
}

func (r *fakeReader) Read(p []byte) (int, error) {
	if r.off >= r.size {
		return 0, io.EOF
	}
	if !r.first {
		r.have = -1
		r.first = true
	}
	n := 0
	for n < len(p) && r.off < r.size {
		idx := r.off / int64(len(r.block))
		if idx != r.have {
			fillBlock(&r.block, r.seed, uint64(idx))
			r.have = idx
		}
		start := int(r.off % int64(len(r.block)))
		c := copy(p[n:], r.block[start:])
		if left := r.size - r.off; int64(c) > left {
			c = int(left)
		}
		n += c
		r.off += int64(c)
	}
	return n, nil
}

func (r *fakeReader) Close() error { return nil }

func fillBlock(buf *[4096]byte, seed, idx uint64) {
	x := seed ^ (idx * 0x9e3779b97f4a7c15)
	for i := 0; i < len(buf); i += 8 {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		binary.LittleEndian.PutUint64(buf[i:], z)
	}
}

func (b *FakeBackend) SetTimes(ctx context.Context, relPath string, mtime, atime, btime time.Time) error {
	e, ok := b.lookup(relPath)
	if !ok {
		return fmt.Errorf("fake: %s: %w", relPath, fs.ErrNotExist)
	}
	if !mtime.IsZero() {
		e.ModTime = mtime
	}
	if !atime.IsZero() {
		e.ATime = atime
	}
	if !btime.IsZero() {
		e.BirthTime = btime
	}
	b.putMut(relPath, &fakeMut{entry: e})
	return nil
}

func (b *FakeBackend) CopyFrom(ctx context.Context, relPath string, src io.Reader, mode os.FileMode) error {
	var size int64
	var buf []byte
	if b.noData {
		n, err := io.Copy(io.Discard, src)
		if err != nil {
			return err
		}
		size = n
	} else {
		data, err := io.ReadAll(src)
		if err != nil {
			return err
		}
		buf, size = data, int64(len(data))
	}
	if mode == 0 {
		mode = 0644
	}
	now := time.Now()
	name := strings.TrimPrefix(relPath[len(model.DirOf(relPath)):], "/")
	b.putMut(relPath, &fakeMut{entry: model.FileEntry{
		RelPath: relPath, Name: name, Size: size, Mode: mode,
		ModTime: now, ATime: now, CTime: now, BirthTime: now,
	}})
	if !b.noData {
		b.mu.Lock()
		b.data[relPath] = buf
		b.mu.Unlock()
	}
	return nil
}

func (b *FakeBackend) Mkdir(ctx context.Context, relPath string, mode os.FileMode) error {
	if mode == 0 {
		mode = 0755
	}
	for p := relPath; p != ""; p = model.DirOf(p) {
		if e, ok := b.lookup(p); ok && e.IsDir {
			break
		}
		now := time.Now()
		name := strings.TrimPrefix(p[len(model.DirOf(p)):], "/")
		b.putMut(p, &fakeMut{entry: model.FileEntry{
			RelPath: p, Name: name, IsDir: true, Mode: os.ModeDir | mode.Perm(),
			ModTime: now, ATime: now, CTime: now, BirthTime: now,
		}})
	}
	return nil
}

func (b *FakeBackend) Rename(ctx context.Context, oldRelPath, newRelPath string) error {
	e, ok := b.lookup(oldRelPath)
	if !ok {
		return fmt.Errorf("fake: %s: %w", oldRelPath, fs.ErrNotExist)
	}
	e.RelPath = newRelPath
	e.Name = strings.TrimPrefix(newRelPath[len(model.DirOf(newRelPath)):], "/")
	b.putMut(oldRelPath, &fakeMut{deleted: true})
	b.putMut(newRelPath, &fakeMut{entry: e})
	b.mu.Lock()
	if data, ok := b.data[oldRelPath]; ok {
		b.data[newRelPath] = data
		delete(b.data, oldRelPath)
	}
	b.mu.Unlock()
	return nil
}

func (b *FakeBackend) Remove(ctx context.Context, relPath string) error {
	if _, ok := b.lookup(relPath); !ok {
		return fmt.Errorf("fake: %s: %w", relPath, fs.ErrNotExist)
	}
	b.putMut(relPath, &fakeMut{deleted: true})
	b.mu.Lock()
	delete(b.data, relPath)
	b.mu.Unlock()
	return nil
}

func (b *FakeBackend) RemoveAll(ctx context.Context, relPath string) error {
	b.putMut(relPath, &fakeMut{deleted: true})
	prefix := relPath + "/"
	b.mu.Lock()
	for p := range b.data {
		if p == relPath || strings.HasPrefix(p, prefix) {
			delete(b.data, p)
		}
	}
	b.mu.Unlock()
	return nil
}
