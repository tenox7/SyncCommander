package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
)

// rclone keeps its config file path, its config password and its signal
// handler for the whole process, so the tests of how sc sets it up (L13)
// run their body in a new process: the test binary again, with only that
// test, in a temp HOME with its own rclone.conf.

const (
	rcloneChildEnv = "SC_RCLONE_CHILD" // the variant a child runs
	rcloneConfEnv  = "SC_RCLONE_CONF"  // the child's rclone.conf
)

// rcloneChild returns the variant the parent asked this process to run, ""
// in the parent. A child reads its rclone.conf at the path the parent gave,
// in a temp dir, never the user's.
func rcloneChild(t *testing.T) string {
	t.Helper()
	v := os.Getenv(rcloneChildEnv)
	if v == "" {
		return ""
	}
	conf := os.Getenv(rcloneConfEnv)
	if conf == "" {
		t.Fatal("no rclone.conf path for the child")
	}
	if err := config.SetConfigPath(conf); err != nil || config.GetConfigPath() != conf {
		t.Fatalf("rclone.conf at %q, want %q: %v", config.GetConfigPath(), conf, err)
	}
	return v
}

type rcloneRun struct {
	stdout, stderr string
	code           int
}

// passed reports whether the child ran the test and it passed: a child
// that matched no test exits 0 too.
func (r rcloneRun) passed(t *testing.T) bool {
	return r.code == 0 && strings.Contains(r.stdout, "--- PASS: "+t.Name()+" (")
}

func (r rcloneRun) String() string {
	return fmt.Sprintf("exit %d\n--- stdout:\n%s--- stderr:\n%s", r.code, r.stdout, r.stderr)
}

// runRcloneChild runs the calling test again in a new process as variant,
// with rclone.conf at conf (which need not exist), HOME in a temp dir, and
// of the RCLONE_ variables only env. Its stdin is empty.
func runRcloneChild(t *testing.T, variant, conf string, env ...string) rcloneRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$",
		"-test.count=1", "-test.v", "-test.timeout=90s")
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		up := strings.ToUpper(name)
		if strings.HasPrefix(up, "RCLONE_") || strings.HasPrefix(up, "_RCLONE_") ||
			up == "HOME" || up == "XDG_CONFIG_HOME" || strings.HasPrefix(up, "SC_RCLONE_") {
			continue
		}
		cmd.Env = append(cmd.Env, kv)
	}
	home := t.TempDir()
	cmd.Env = append(cmd.Env, rcloneChildEnv+"="+variant, rcloneConfEnv+"="+conf,
		"HOME="+home, "XDG_CONFIG_HOME="+home, "RCLONE_CONFIG="+conf)
	cmd.Env = append(cmd.Env, env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	r := rcloneRun{stdout: stdout.String(), stderr: stderr.String()}
	var ee *exec.ExitError
	switch {
	case errors.As(err, &ee):
		r.code = ee.ExitCode()
	case err != nil:
		t.Fatalf("child: %v", err)
	}
	return r
}

// writeEncryptedRcloneConf writes plain as the child's rclone.conf,
// encrypted with pass as `rclone config encryption set` does, and forgets
// the key: the load has to find it on its own.
func writeEncryptedRcloneConf(t *testing.T, plain, pass string) {
	t.Helper()
	if err := config.SetConfigPassword(pass); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := config.Encrypt(strings.NewReader(plain), &buf); err != nil {
		t.Fatal(err)
	}
	config.ClearConfigPassword()
	if err := os.WriteFile(config.GetConfigPath(), buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

// An encrypted rclone.conf with no password to hand fails every rclone
// open with rclone's advice, and nothing asks on the terminal, which the TUI
// holds (L13). Before, the open printed rclone's prompt and read stdin, and
// at the end of stdin rclone ended the process.
func TestRcloneEncryptedConfigAsksNoPassword(t *testing.T) {
	if rcloneChild(t) != "" {
		writeEncryptedRcloneConf(t, "[enc]\ntype = memory\n", "sc-test-password")
		const want = "RCLONE_CONFIG_PASS"
		// The open inside the TUI, then the check before it starts.
		if _, err := NewRcloneBackend(context.Background(), "rclone://enc/bkt", false); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("open: %v, want rclone's advice to set %s", err, want)
		}
		if _, err := TryOpenBackend("rclone://enc/bkt", false, 1); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("check before the TUI: %v, want rclone's advice to set %s", err, want)
		}
		// Every remote reads the file, as in rclone itself.
		if _, err := NewRcloneBackend(context.Background(), "rclone://:memory:bkt", false); err == nil {
			t.Fatal("a remote opened beside a config file that cannot be read")
		}
		return
	}
	conf := filepath.Join(t.TempDir(), "rclone.conf")
	// The user may ask rclone to ask; inside the TUI it cannot.
	r := runRcloneChild(t, "child", conf, "RCLONE_ASK_PASSWORD=true")
	if !r.passed(t) || strings.Contains(r.stderr, "password") {
		t.Fatalf("an encrypted rclone.conf with no password: %v", r)
	}
}

// An encrypted rclone.conf still opens with its password in
// RCLONE_CONFIG_PASS, or from RCLONE_PASSWORD_COMMAND.
func TestRcloneEncryptedConfigTakesItsPasswordFromTheEnvironment(t *testing.T) {
	if rcloneChild(t) != "" {
		writeEncryptedRcloneConf(t, "[enc]\ntype = memory\n", "sc-test-password")
		if _, err := TryOpenBackend("rclone://enc/bkt", false, 1); err != nil {
			t.Fatal(err)
		}
		b, err := NewRcloneBackend(context.Background(), "rclone://enc/bkt", false)
		if err != nil {
			t.Fatal(err)
		}
		_ = b.Close()
		return
	}
	conf := filepath.Join(t.TempDir(), "rclone.conf")
	if r := runRcloneChild(t, "child", conf, "RCLONE_CONFIG_PASS=sc-test-password"); !r.passed(t) {
		t.Fatalf("RCLONE_CONFIG_PASS: %v", r)
	}
	if runtime.GOOS == "windows" {
		return // echo is no program there
	}
	if r := runRcloneChild(t, "child", conf, "RCLONE_PASSWORD_COMMAND=echo sc-test-password"); !r.passed(t) {
		t.Fatalf("RCLONE_PASSWORD_COMMAND: %v", r)
	}
}

// A config file rclone cannot read fails the open with the reason, where
// rclone itself ended the process (L13), and a file fixed while sc runs is
// read again by the next open.
func TestRcloneUnreadableConfigFailsTheOpen(t *testing.T) {
	if rcloneChild(t) != "" {
		const want = "unsupported configuration encryption"
		if _, err := NewRcloneBackend(context.Background(), "rclone://mem/bkt", false); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("open: %v, want %q", err, want)
		}
		if _, err := TryOpenBackend("rclone://mem/bkt", false, 1); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("check before the TUI: %v, want %q", err, want)
		}
		if err := os.WriteFile(config.GetConfigPath(), []byte("[mem]\ntype = memory\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		b, err := NewRcloneBackend(context.Background(), "rclone://mem/bkt", false)
		if err != nil {
			t.Fatalf("open after the file was fixed: %v", err)
		}
		_ = b.Close()
		return
	}
	conf := filepath.Join(t.TempDir(), "rclone.conf")
	// The header of a newer encryption: a file this rclone cannot read.
	if err := os.WriteFile(conf, []byte("RCLONE_ENCRYPT_V9:\nAAAA\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if r := runRcloneChild(t, "child", conf); !r.passed(t) {
		t.Fatalf("a config file rclone cannot read: %v", r)
	}
}

// rclone still ends the process on a few errors it calls fatal, such as a
// CA certificate file it cannot read. sc cannot stop that exit, but the
// terminal is given back first, and the reason goes to stderr, where sc's
// own log would take it away with the process (L13).
func TestRcloneFatalGivesTheTerminalBack(t *testing.T) {
	if v := rcloneChild(t); v != "" {
		OnRcloneFatal(func() { fmt.Println("terminal given back") })
		if v == "after the TUI" {
			OnRcloneFatal(nil) // as main does once the TUI has ended
		}
		_, err := NewRcloneBackend(context.Background(), "rclone://:http,url='http://127.0.0.1:9/':", false)
		t.Fatalf("rclone did not end the process: %v", err)
	}
	dir := t.TempDir()
	conf := filepath.Join(dir, "rclone.conf")
	cert := "RCLONE_CA_CERT=" + filepath.Join(dir, "missing.pem")
	r := runRcloneChild(t, "in the TUI", conf, cert)
	if r.code != 1 || !strings.Contains(r.stdout, "terminal given back") ||
		!strings.Contains(r.stderr, "sc: rclone ended the program") || !strings.Contains(r.stderr, "missing.pem") {
		t.Fatalf("a fatal rclone error inside the TUI: %v", r)
	}
	r = runRcloneChild(t, "after the TUI", conf, cert)
	if r.code != 1 || strings.Contains(r.stdout, "terminal given back") || !strings.Contains(r.stderr, "missing.pem") {
		t.Fatalf("a fatal rclone error after the TUI: %v", r)
	}
}

// Only rclone's fatal level, which comes right before its os.Exit, gives
// the terminal back: after an error, or a panic someone may recover, sc
// goes on.
func TestRcloneLogGivesTheTerminalBackOnlyBeforeAnExit(t *testing.T) {
	given := 0
	OnRcloneFatal(func() { given++ })
	defer OnRcloneFatal(nil)
	for _, l := range []slog.Level{slog.LevelError, fs.SlogLevelAlert, fs.SlogLevelEmergency} {
		_ = rcloneLogHandler{}.Handle(context.Background(), slog.NewRecord(time.Now(), l, "test record", 0))
	}
	if given != 0 {
		t.Fatalf("the terminal was given back %d times with no exit to come", given)
	}
}
