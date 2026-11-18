package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"sc/model"
)

// testServerCert is a self-signed certificate for 127.0.0.1.
func testServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// L18: an FTPS data connection never resumed the TLS session of its control
// connection: the config had no session cache, and Go resumes none without
// one. vsftpd (require_ssl_reuse), ProFTPD and FileZilla Server refuse such
// a data connection by default: login worked, the first listing failed.
// Now a listing, a download and an upload each resume it, over TLS 1.2 and
// 1.3, implicit (ftps://) and after AUTH TLS (ftpes://). Each control
// connection of the fake has ticket keys of its own, so a data connection
// that took the session of another control connection would be new: the
// listing runs on the primary, the download and the upload on an extra.
func TestFTPSDataConnectionsResumeTheSession(t *testing.T) {
	cert := testServerCert(t)
	for _, scheme := range []string{"ftps", "ftpes"} {
		for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
			t.Run(scheme+"/"+tls.VersionName(version), func(t *testing.T) {
				f := &fakeFTPD{
					ls:          map[string][]string{"/": {lsLine("a.bin", false)}},
					files:       map[string]fakeFTPFile{"/a.bin": {"abc", "226 Transfer complete."}},
					store:       map[string][]byte{},
					tlsCfg:      &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: version, MaxVersion: version},
					implicitTLS: scheme == "ftps",
				}
				addr := f.start(t)
				ctx := context.Background()
				b, err := NewFTPBackend(ctx, scheme+"://u:p@"+addr+"/", true, 2)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					f.closeAll()
					b.Close()
				})
				if _, err := b.List(ctx, ""); err != nil {
					t.Errorf("List: %v", err)
				}
				r, err := b.Open(ctx, "a.bin")
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				if err != nil || string(got) != "abc" {
					t.Errorf("download: %q, %v", got, err)
				}
				if err := r.Close(); err != nil {
					t.Errorf("Close of the download: %v", err)
				}
				if err := b.CopyFrom(ctx, "up.txt", strings.NewReader("up"), model.PutParams{Size: 2}); err != nil {
					t.Errorf("upload: %v", err)
				}
				f.mu.Lock()
				conns, stored := slices.Clone(f.dataTLS), string(f.store["/up.txt"])
				f.mu.Unlock()
				if stored != "up" {
					t.Errorf("the upload stored %q, want up", stored)
				}
				for _, cmd := range []string{"LIST", "RETR", "STOR"} {
					if !slices.ContainsFunc(conns, func(c string) bool { return strings.HasPrefix(c, cmd+" ") }) {
						t.Errorf("no %s data connection: %q", cmd, conns)
					}
				}
				for _, c := range conns {
					if !strings.HasSuffix(c, " resumed") {
						t.Errorf("a data connection did not resume the TLS session: %s (all: %q)", c, conns)
					}
				}
			})
		}
	}
}
