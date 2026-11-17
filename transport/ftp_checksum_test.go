package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
	"time"

	"sc/model"
)

// setFTPHashRedial sets, for one test, the wait after a failed dial of the
// checksum connection.
func setFTPHashRedial(t *testing.T, d time.Duration) {
	t.Helper()
	old, oldMax := ftpHashRedial, ftpHashRedialMax
	ftpHashRedial, ftpHashRedialMax = d, d
	t.Cleanup(func() { ftpHashRedial, ftpHashRedialMax = old, oldMax })
}

func sha256Hex(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// hashFTPD is a server that sums with XSHA256. Its first connection is the
// primary, its second the checksum connection made at connect.
func hashFTPD() *fakeFTPD {
	return &fakeFTPD{
		ls:     map[string][]string{"/": {lsLine("a.txt", false), lsLine("b.txt", false), lsLine("c.txt", false)}},
		store:  map[string][]byte{"/a.txt": []byte("alpha"), "/b.txt": []byte("bravo"), "/c.txt": []byte("charlie")},
		hashes: true,
	}
}

var hashBodies = map[string]string{"a.txt": "alpha", "b.txt": "bravo", "c.txt": "charlie"}

// F24: the connection that asks an FTP server for checksums was made once,
// at connect. When that failed, as on a server at its connection limit, the
// backend said the server had no checksums, with no error, and the scanner
// kept that answer for the session. Now an ask with no connection dials
// one, and one that cannot be made is an error, which the scanner asks
// again.
func TestFTPChecksumConnectionThatFailedIsDialedAgain(t *testing.T) {
	setFTPHashRedial(t, 0)
	f := hashFTPD()
	f.refuse = func(n int) bool { return n == 2 || n == 3 }
	b := openFakeFTP(t, f, "/")
	ctx := context.Background()
	if algos, err := b.ChecksumAlgos(ctx); err == nil {
		t.Fatalf("with no checksum connection the server was said to have the checksums %q, with no error: the scanner keeps that for the session", algos)
	}
	algos, err := b.ChecksumAlgos(ctx)
	if err != nil || !slices.Contains(algos, "sha256") {
		t.Fatalf("once the server takes the connection its checksums are %q, %v; want sha256", algos, err)
	}
	if sum, err := b.Checksum(ctx, "a.txt", "sha256"); err != nil || sum != sha256Hex("alpha") {
		t.Errorf("sha256 of a.txt: %q, %v; want %s", sum, err, sha256Hex("alpha"))
	}
}

// F24: a checksum connection whose FEAT got no answer, the server closing
// it, was taken for a server with no hash command, for good. Only a FEAT
// the server answered says that now.
func TestFTPChecksumConnectionWithNoFEATAnswerIsDialedAgain(t *testing.T) {
	setFTPHashRedial(t, 0)
	f := hashFTPD()
	f.featFails = func(n int) bool { return n == 2 }
	b := openFakeFTP(t, f, "/")
	algos, err := b.ChecksumAlgos(context.Background())
	if err != nil || !slices.Contains(algos, "sha256") {
		t.Errorf("after a FEAT the server did not answer, its checksums are %q, %v; want sha256", algos, err)
	}
}

// A stopped dial is no failure of the server's: it starts no wait, and the
// next ask dials.
func TestFTPStoppedChecksumDialStartsNoWait(t *testing.T) {
	setFTPHashRedial(t, 0)
	f := hashFTPD()
	f.refuse = func(n int) bool { return n == 2 }
	b := openFakeFTP(t, f, "/")
	setFTPHashRedial(t, time.Hour)
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.ChecksumAlgos(stopped); !errors.Is(err, context.Canceled) {
		t.Fatalf("a stopped ask ended with %v, want the stop", err)
	}
	algos, err := b.ChecksumAlgos(context.Background())
	if err != nil || !slices.Contains(algos, "sha256") {
		t.Errorf("after a stopped dial the checksums are %q, %v; want sha256: the stop started a wait", algos, err)
	}
}

// The wait after a failed dial doubles while dials fail, up to the most,
// and a dial that works ends it.
func TestFTPChecksumRedialWaitDoubles(t *testing.T) {
	old, oldMax := ftpHashRedial, ftpHashRedialMax
	ftpHashRedial, ftpHashRedialMax = time.Second, 5*time.Second
	t.Cleanup(func() { ftpHashRedial, ftpHashRedialMax = old, oldMax })
	c := newFTPHashConn(nil, nil, errors.New("refused"))
	var waits []time.Duration
	fail := func() {
		c.tookLocked(nil, errors.New("refused"))
		waits = append(waits, c.wait)
	}
	waits = append(waits, c.wait)
	fail()
	fail()
	fail()
	c.tookLocked(&ftpHasher{}, nil)
	fail()
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, time.Second}
	if !slices.Equal(waits, want) {
		t.Errorf("the waits after failed dials are %v, want %v", waits, want)
	}
}

// F24: a checksum connection that died, as an idle one does while a long
// listing runs on the primary, failed every checksum after it: the backend
// asked on it again each time. Now the ask that finds it dead dials a new
// one and asks once more, and no file fails.
func TestFTPChecksumConnectionThatDiedIsDialedAgain(t *testing.T) {
	f := hashFTPD()
	b := openFakeFTP(t, f, "/")
	ctx := context.Background()
	if sum, err := b.Checksum(ctx, "a.txt", "sha256"); err != nil || sum != sha256Hex("alpha") {
		t.Fatalf("sha256 of a.txt: %q, %v", sum, err)
	}
	f.kill(2)
	for _, name := range []string{"b.txt", "c.txt"} {
		if sum, err := b.Checksum(ctx, name, "sha256"); err != nil || sum != sha256Hex(hashBodies[name]) {
			t.Errorf("after the server ended the idle checksum connection, sha256 of %s: %q, %v", name, sum, err)
		}
	}
	if n := f.connCount(); n != 3 {
		t.Errorf("%d connections, want 3: the primary, the checksum connection, the one dialed after it died", n)
	}
}

// F24: an ask for a checksum could not be stopped. A server that did not
// answer held a stopped checksum pass for good. A stop now ends the ask,
// and the next one gets a new connection.
func TestFTPChecksumAskStops(t *testing.T) {
	f := hashFTPD()
	f.hangOn, f.hangAsked = "/a.txt", make(chan struct{}, 1)
	b := openFakeFTP(t, f, "/")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.Checksum(ctx, "a.txt", "sha256")
		done <- err
	}()
	<-f.hangAsked
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("a stopped ask ended with %v, want the stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stopped ask still waits for the server after 5 s")
	}
	if sum, err := b.Checksum(context.Background(), "b.txt", "sha256"); err != nil || sum != sha256Hex("bravo") {
		t.Errorf("after a stopped ask, sha256 of b.txt: %q, %v", sum, err)
	}
}

// A server whose FEAT names no hash command has none: asking again dials
// nothing. One that turns the checksum connection away is dialed again only
// after a wait, not once per file.
func TestFTPChecksumConnectionIsNotDialedPerAsk(t *testing.T) {
	ctx := context.Background()
	t.Run("no hash command", func(t *testing.T) {
		setFTPHashRedial(t, 0)
		f := hashFTPD()
		f.hashes = false
		b := openFakeFTP(t, f, "/")
		for range 3 {
			if algos, err := b.ChecksumAlgos(ctx); err != nil || len(algos) != 0 {
				t.Fatalf("a server with no hash command has the checksums %q, %v; want none", algos, err)
			}
		}
		if n := f.connCount(); n != 2 {
			t.Errorf("%d connections, want 2: a server with no hash command was asked again", n)
		}
	})
	t.Run("turned away", func(t *testing.T) {
		setFTPHashRedial(t, time.Hour)
		f := hashFTPD()
		f.refuse = func(n int) bool { return n > 1 }
		b := openFakeFTP(t, f, "/")
		for range 3 {
			if algos, err := b.ChecksumAlgos(ctx); err == nil {
				t.Fatalf("with the checksum connection turned away the server has the checksums %q, with no error", algos)
			}
			if sum, err := b.Checksum(ctx, "a.txt", "sha256"); err == nil {
				t.Fatalf("with the checksum connection turned away, a sum: %q", sum)
			}
		}
		if n := f.connCount(); n != 2 {
			t.Errorf("%d connections, want 2: the server that turned one away was dialed again before the wait", n)
		}
	})
	t.Run("closed", func(t *testing.T) {
		setFTPHashRedial(t, 0)
		f := hashFTPD()
		b := openFakeFTP(t, f, "/")
		f.kill(2)
		b.Close()
		if sum, err := b.Checksum(ctx, "a.txt", "sha256"); err == nil {
			t.Errorf("a closed backend summed a.txt: %q", sum)
		}
		if algos, err := b.ChecksumAlgos(ctx); err == nil {
			t.Errorf("a closed backend has the checksums %q, with no error", algos)
		}
		if n := f.connCount(); n != 2 {
			t.Errorf("%d connections, want 2: a closed backend dialed", n)
		}
	})
}

// A failed checksum connection says nothing of the primary: the connection
// wrapper must neither hold off the backend's calls as unreachable, for a
// dial that failed, nor close the backend as lost, for a connection that
// died. Listings go on, on the same primary connection.
func TestFTPChecksumConnectionFailureLeavesThePrimary(t *testing.T) {
	SetMaxRetries(1)
	t.Cleanup(func() { SetMaxRetries(5) })
	setFTPHashRedial(t, 0)
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		break_ func(f *fakeFTPD)
	}{
		{"no dial", func(f *fakeFTPD) { f.listener.Close(); f.kill(2) }},
		{"dies on each sum", func(f *fakeFTPD) {
			f.mu.Lock()
			f.dropOnSum = true
			f.mu.Unlock()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := hashFTPD()
			addr := f.start(t)
			lb := NewLazyBackend("ftp://"+addr+"/", func(ctx context.Context) (model.Backend, error) {
				return NewFTPBackend(ctx, "ftp://u:p@"+addr+"/", false, 1)
			})
			t.Cleanup(func() { lb.Close() })
			if _, err := lb.ChecksumAlgos(ctx); err != nil {
				t.Fatal(err)
			}
			tc.break_(f)
			for range 3 {
				if sum, err := lb.Checksum(ctx, "a.txt", "sha256"); err == nil {
					t.Fatalf("with no checksum connection to be had, a sum: %q", sum)
				}
			}
			entries, err := lb.List(ctx, "")
			if err != nil || len(entries) != 3 {
				t.Fatalf("after the checksum connection failed, the primary listed %d entries, %v; want 3", len(entries), err)
			}
			f.mu.Lock()
			conns := slices.Clone(f.listConns)
			f.mu.Unlock()
			if slices.ContainsFunc(conns, func(n int) bool { return n != 1 }) {
				t.Errorf("listings came on the connections %v, want 1 only: the backend was closed and connected again", conns)
			}
		})
	}
}
