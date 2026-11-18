package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"

	"sc/model"
)

// bucketStore is a store as S3 is, made of rclone's memory one: it renames
// no folder and moves no file; it copies on the server, unless made with
// noCopy. From the gateAt-th copy on, each copy waits until gate closes.
// A copy of failCopyOf fails. Its objects refuse a delete whose ctx ended,
// as a store's request does. A listing of failDir fails failLists times;
// one of extraIn also gives extra. lists counts the listings of listsUnder
// and below; rmdirs keeps the folders removed. With folders, it has
// folders of its own: a copy into one not made fails.
type bucketStore struct {
	fs.Fs
	feat       *fs.Features
	copies     atomic.Int32
	gateAt     int32
	gate       chan struct{}
	entered    chan struct{}
	enterOnce  sync.Once
	failDir    string
	failLists  atomic.Int32
	extraIn    string
	extra      fs.DirEntry
	listsUnder string
	lists      atomic.Int32
	failCopyOf string
	folders    bool
	mu         sync.Mutex
	rmdirs     []string
	made       map[string]bool
}

// storeObject is an object of a bucketStore.
type storeObject struct{ fs.Object }

func (o storeObject) Remove(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return o.Object.Remove(ctx)
}

// renamedObject is an object listed under a name not its own.
type renamedObject struct {
	fs.Object
	remote string
}

func (o renamedObject) Remote() string { return o.remote }
func (o renamedObject) String() string { return o.remote }

// memoryObject is the memory store's own object under o, the only kind it
// copies.
func memoryObject(o fs.Object) fs.Object {
	for {
		switch w := o.(type) {
		case storeObject:
			o = w.Object
		case renamedObject:
			o = w.Object
		default:
			return o
		}
	}
}

func (s *bucketStore) Features() *fs.Features { return s.feat }

func (s *bucketStore) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := s.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return storeObject{o}, nil
}

func (s *bucketStore) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if s.listsUnder != "" && (dir == s.listsUnder || strings.HasPrefix(dir, s.listsUnder+"/")) {
		s.lists.Add(1)
	}
	if dir == s.failDir && s.failLists.Add(-1) >= 0 {
		return nil, errors.New("the listing was cut off")
	}
	entries, err := s.Fs.List(ctx, dir)
	for i, e := range entries {
		if o, ok := e.(fs.Object); ok {
			entries[i] = storeObject{o}
		}
	}
	if err == nil && dir == s.extraIn && s.extra != nil {
		entries = append(entries, s.extra)
	}
	return entries, err
}

func (s *bucketStore) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	if n := s.copies.Add(1); s.gate != nil && n >= s.gateAt {
		s.enterOnce.Do(func() { close(s.entered) })
		<-s.gate
	}
	if memoryObject(src).Remote() == s.failCopyOf {
		return nil, errors.New("the copy failed")
	}
	if dir := parentDir(remote); s.folders && dir != "" {
		s.mu.Lock()
		made := s.made[dir]
		s.mu.Unlock()
		if !made {
			return nil, fmt.Errorf("%s: no folder %s", remote, dir)
		}
	}
	o, err := s.Fs.Features().Copy(ctx, memoryObject(src), remote)
	if err != nil {
		return nil, err
	}
	return storeObject{o}, nil
}

func (s *bucketStore) Mkdir(ctx context.Context, dir string) error {
	s.mu.Lock()
	if s.made == nil {
		s.made = map[string]bool{}
	}
	s.made[dir] = true
	s.mu.Unlock()
	return s.Fs.Mkdir(ctx, dir)
}

func (s *bucketStore) Rmdir(ctx context.Context, dir string) error {
	s.mu.Lock()
	s.rmdirs = append(s.rmdirs, dir)
	s.mu.Unlock()
	return s.Fs.Rmdir(ctx, dir)
}

var bucketStores atomic.Int32

// newBucketStore makes an rclone backend on a new memory bucket seen
// through a bucketStore, holding each file with the body "body of" it.
func newBucketStore(t *testing.T, noCopy bool, files ...string) (*RcloneBackend, *bucketStore) {
	t.Helper()
	ctx := context.Background()
	b, err := NewRcloneBackend(ctx, fmt.Sprintf("rclone://:memory:l21-%d", bucketStores.Add(1)), false)
	if err != nil {
		t.Fatal(err)
	}
	s := &bucketStore{Fs: b.f}
	s.feat = (&fs.Features{}).Fill(ctx, s)
	if noCopy {
		s.feat.Copy = nil
	}
	b.f, b.feat = s, s.feat
	for _, f := range files {
		body := "body of " + f
		if err := b.CopyFrom(ctx, f, strings.NewReader(body), model.PutParams{Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
	}
	return b, s
}

// objectsIn lists, on the memory store itself, the objects under dir.
func objectsIn(t *testing.T, f fs.Fs, dir string) []string {
	t.Helper()
	entries, err := f.List(context.Background(), dir)
	if errors.Is(err, fs.ErrorDirNotFound) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		switch e.(type) {
		case fs.Directory:
			names = append(names, objectsIn(t, f, e.Remote())...)
		case fs.Object:
			names = append(names, e.Remote())
		}
	}
	slices.Sort(names)
	return names
}

// bodyOf reads name on the memory store itself; "" when it is not there.
func bodyOf(t *testing.T, f fs.Fs, name string) string {
	t.Helper()
	ctx := context.Background()
	o, err := f.NewObject(ctx, name)
	if err != nil {
		return ""
	}
	r, err := o.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// loggedSince reports a log line since mark that holds want.
func loggedSince(mark int, want string) bool {
	lines := Log.Lines()
	return slices.ContainsFunc(lines[min(mark, len(lines)):], func(l string) bool { return strings.Contains(l, want) })
}

// L21: a folder rename on a store with no folder rename of its own (S3,
// B2, Swift) ran inside rclone under context.Background(): a stop did
// nothing, and the rename went on, for hours on a large folder. Now it
// stops between objects: those moved are in the new folder, the rest in
// the old one, each file in one place, and the log says how many moved.
func TestRcloneFolderRenameStopsWhenAsked(t *testing.T) {
	var files []string
	for i := range 40 {
		files = append(files, fmt.Sprintf("old/%c/%02d.txt", 'a'+i%2, i))
	}
	b, s := newBucketStore(t, false, files...)
	s.gateAt, s.gate, s.entered = 5, make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mark := len(Log.Lines())
	done := make(chan error, 1)
	go func() { done <- b.Rename(ctx, "old", "new") }()
	select {
	case <-s.entered:
	case <-time.After(10 * time.Second):
		close(s.gate)
		t.Fatal("the rename made no fifth copy")
	}
	cancel()
	close(s.gate)
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the rename went on after the stop")
	}
	moved, left := objectsIn(t, s.Fs, "new"), objectsIn(t, s.Fs, "old")
	if err == nil {
		t.Errorf("the rename said OK after a stop, having moved %d of %d", len(moved), len(files))
	}
	// 4 copies ran before the gate, and each mover can have one waiting.
	if len(moved) > 4+rcloneMoveWorkers {
		t.Errorf("%d of %d moved: the rename went on after the stop", len(moved), len(files))
	}
	var each []string
	for _, m := range moved {
		each = append(each, "old/"+strings.TrimPrefix(m, "new/"))
	}
	each = append(each, left...)
	slices.Sort(each)
	if want := slices.Sorted(slices.Values(files)); !slices.Equal(each, want) {
		t.Errorf("moved %q and left %q; want each file in one place", moved, left)
	}
	if want := fmt.Sprintf("rename old -> new stopped after %d objects", len(moved)); !loggedSince(mark, want) {
		t.Errorf("no log line %q", want)
	}
}

// L21: on a store that can neither move nor copy on the server, rclone
// renamed by downloading each file and uploading it again: around sc's
// bandwidth limits, with no progress shown. Such a rename is refused now,
// with the reason, before the folder is listed or anything is written.
func TestRcloneRenameNeverDownloadsAndUploads(t *testing.T) {
	files := []string{"f.txt", "old/a.txt", "old/sub/b.txt"}
	b, s := newBucketStore(t, true, files...)
	s.listsUnder = "old"
	ctx := context.Background()
	for _, c := range []struct{ from, to string }{{"f.txt", "g.txt"}, {"old", "new"}} {
		err := b.Rename(ctx, c.from, c.to)
		if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "neither move nor copy on the server") {
			t.Errorf("rename %s: %v; want it refused: the store can neither move nor copy", c.from, err)
		}
	}
	if got := objectsIn(t, s.Fs, ""); !slices.Equal(got, files) {
		t.Errorf("the store holds %q; want it as it was, %q", got, files)
	}
	if n := s.lists.Load(); n != 0 {
		t.Errorf("the refused rename listed the folder first (%d listings)", n)
	}
}

// L21: rclone deletes a folder on a store with no purge of its own by
// deleting what its listing gives. A listing that failed part-way was
// logged, and the delete said OK with files left behind. Now a delete that
// leaves something fails and says what is left.
func TestRcloneDeleteThatLeavesFilesSaysSo(t *testing.T) {
	old := purgeRecheck
	purgeRecheck = 0
	defer func() { purgeRecheck = old }()
	files := []string{"keep.txt", "old/a/1.txt", "old/a/2.txt", "old/b/3.txt", "old/b/4.txt"}
	b, s := newBucketStore(t, false, files...)
	s.failDir = "old/b"
	s.failLists.Store(1)
	ctx := context.Background()
	err := b.RemoveAll(ctx, "old")
	left := objectsIn(t, s.Fs, "old")
	if len(left) == 0 {
		t.Fatalf("the delete's listing did not fail as set up: nothing is left (%v)", err)
	}
	if err == nil || !strings.Contains(err.Error(), "still holds") {
		t.Errorf("the delete left %q and said %v; want it to fail and say so", left, err)
	}
	if err := b.RemoveAll(ctx, "old"); err != nil {
		t.Errorf("a delete that leaves nothing: %v", err)
	}
	if got := objectsIn(t, s.Fs, ""); !slices.Equal(got, []string{"keep.txt"}) {
		t.Errorf("the store holds %q after the second delete; want only keep.txt", got)
	}
}

// L21: the object cache kept the files of a folder renamed or deleted:
// Checksum and Open answered for files that were gone.
func TestRcloneCacheForgetsAFolderMovedOrDeleted(t *testing.T) {
	b, _ := newBucketStore(t, false, "old/a.txt", "gone/b.txt")
	ctx := context.Background()
	for _, rel := range []string{"old/a.txt", "gone/b.txt"} {
		if _, err := b.Checksum(ctx, rel, "md5"); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Rename(ctx, "old", "new"); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveAll(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"old/a.txt", "gone/b.txt"} {
		if sum, err := b.Checksum(ctx, rel, "md5"); err == nil {
			t.Errorf("%s has the sum %s after it was moved or deleted; want none", rel, sum)
		}
		if r, err := b.Open(ctx, rel); err == nil {
			r.Close()
			t.Errorf("%s opens after it was moved or deleted", rel)
		}
	}
	if _, err := b.Checksum(ctx, "new/a.txt", "md5"); err != nil {
		t.Errorf("new/a.txt: %v", err)
	}
}

// L21: a folder rename object by object says how far it got, every so
// many objects, and when it is done; the files arrive whole.
func TestRcloneFolderRenameObjectByObject(t *testing.T) {
	old := rcloneMoveLogEvery
	rcloneMoveLogEvery = 10
	defer func() { rcloneMoveLogEvery = old }()
	var files []string
	for i := range 25 {
		files = append(files, fmt.Sprintf("old/%c/%02d.txt", 'a'+i%3, i))
	}
	b, s := newBucketStore(t, false, files...)
	mark := len(Log.Lines())
	if err := b.Rename(context.Background(), "old", "new"); err != nil {
		t.Fatal(err)
	}
	if left := objectsIn(t, s.Fs, "old"); len(left) != 0 {
		t.Errorf("left in old: %q", left)
	}
	for _, f := range files {
		moved := "new/" + strings.TrimPrefix(f, "old/")
		if got := bodyOf(t, s.Fs, moved); got != "body of "+f {
			t.Errorf("%s holds %q; want the body of %s", moved, got, f)
		}
	}
	for _, want := range []string{"rename old -> new: 10 objects moved", "rename old -> new: 20 objects moved", "rename old -> new: done, 25 objects moved"} {
		if !loggedSince(mark, want) {
			t.Errorf("no log line %q", want)
		}
	}
	// On a store with folders of its own, the old ones are left empty.
	s.mu.Lock()
	rmdirs := slices.Clone(s.rmdirs)
	s.mu.Unlock()
	if n := len(rmdirs); n != 4 || rmdirs[3] != "old" || !slices.Equal(slices.Sorted(slices.Values(rmdirs[:3])), []string{"old/a", "old/b", "old/c"}) {
		t.Errorf("folders removed: %q; want old/a, old/b and old/c, then old", rmdirs)
	}
}

// L21: on a store with folders of its own, the rename makes each new
// folder before it moves a file into it: new itself, which the listing of
// old does not name, and each folder below it.
func TestRcloneFolderRenameMakesItsFolders(t *testing.T) {
	files := []string{"old/1.txt", "old/2.txt", "old/s/3.txt"}
	b, s := newBucketStore(t, false, files...)
	s.folders = true
	if err := b.Rename(context.Background(), "old", "new"); err != nil {
		t.Fatal(err)
	}
	if got, want := objectsIn(t, s.Fs, "new"), []string{"new/1.txt", "new/2.txt", "new/s/3.txt"}; !slices.Equal(got, want) {
		t.Errorf("new holds %q, want %q", got, want)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dir := range []string{"new", "new/s"} {
		if !s.made[dir] {
			t.Errorf("the folder %s was not made", dir)
		}
	}
}

// L21: a move that fails stops the folder rename with its error: each file
// is then in one place, the one that failed where it was.
func TestRcloneFolderRenameStopsAtAFailedMove(t *testing.T) {
	var files []string
	for i := range 40 {
		files = append(files, fmt.Sprintf("old/%02d.txt", i))
	}
	b, s := newBucketStore(t, false, files...)
	s.failCopyOf = "old/05.txt"
	done := make(chan error, 1)
	go func() { done <- b.Rename(context.Background(), "old", "new") }()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the rename did not end after a move failed")
	}
	if err == nil || !strings.Contains(err.Error(), "the copy failed") {
		t.Errorf("rename: %v; want the failed copy", err)
	}
	moved, left := objectsIn(t, s.Fs, "new"), objectsIn(t, s.Fs, "old")
	if !slices.Contains(left, "old/05.txt") {
		t.Errorf("old/05.txt, whose copy failed, is not where it was: left %q", left)
	}
	var each []string
	for _, m := range moved {
		each = append(each, "old/"+strings.TrimPrefix(m, "new/"))
	}
	each = append(each, left...)
	slices.Sort(each)
	if !slices.Equal(each, files) {
		t.Errorf("moved %q and left %q; want each file in one place", moved, left)
	}
}

// L21: a store can hold an object named .. in a folder (the S3 key
// old/..), and rclone's listing lets it through: it drops only names that
// climb above the root or hold a slash. Moved by its name, it went to
// new/.., the folder above new: outside the folder. Such a name stops the
// rename now, and nothing outside the two folders is written.
func TestRcloneFolderRenameKeepsToItsFolder(t *testing.T) {
	files := []string{"keep.txt", "old/a.txt", "old/b.txt"}
	b, s := newBucketStore(t, false, files...)
	ctx := context.Background()
	a, err := s.NewObject(ctx, "old/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	s.extraIn, s.extra = "old", renamedObject{a, "old/.."}
	err = b.Rename(ctx, "old", "new")
	if err == nil || !strings.Contains(err.Error(), `"old/.."`) {
		t.Errorf("rename: %v; want it stopped at the name old/..", err)
	}
	if got := bodyOf(t, s.Fs, "keep.txt"); got != "body of keep.txt" {
		t.Errorf("keep.txt holds %q: the rename wrote outside its folders", got)
	}
	for _, name := range objectsIn(t, s.Fs, "") {
		if name != "keep.txt" && !strings.HasPrefix(name, "old/") && !strings.HasPrefix(name, "new/") {
			t.Errorf("the rename wrote %s, outside its folders", name)
		}
	}
}
