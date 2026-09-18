package transport

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"

	rsyncpkg "github.com/gokrazy/rsync"
	"github.com/gokrazy/rsync/rsyncclient"
	"github.com/mmcloughlin/md4"
)

// daemonHandshake runs the text part of the rsync daemon protocol on conn up
// to where client.Run takes over: greeting, module name, the AUTHREQD
// challenge and the server argument list. gorsync's own RunDaemon reads the
// username from a process-wide environment variable, so two panels with
// different daemon users would authenticate as whichever opened last; doing
// the exchange here keeps the credentials per backend. Reports done when the
// daemon answered EXIT (nothing to run).
func (b *RsyncBackend) daemonHandshake(ctx context.Context, conn io.ReadWriter, client *rsyncclient.Client, remotePath string) (done bool, err error) {
	module, _, _ := strings.Cut(remotePath, "/")
	rd := bufio.NewReader(conn)
	if _, err := fmt.Fprintf(conn, "@RSYNCD: %d\n", rsyncpkg.ProtocolVersion); err != nil {
		return false, err
	}
	greeting, err := readLine(rd)
	if err != nil {
		return false, fmt.Errorf("reading daemon greeting: %w", err)
	}
	if v, err := daemonProtocol(greeting); err != nil {
		return false, err
	} else if v < 27 {
		return false, fmt.Errorf("daemon protocol %d too old", v)
	}
	if _, err := fmt.Fprintf(conn, "%s\n", module); err != nil {
		return false, err
	}
	for {
		line, err := readLine(rd)
		if err != nil {
			return false, fmt.Errorf("daemon startup: %w", err)
		}
		switch {
		case strings.HasPrefix(line, "@RSYNCD: AUTHREQD "):
			if b.pass == "" {
				return false, errors.New("daemon requires a password: use rsync://user:pass@host/module")
			}
			challenge := strings.TrimPrefix(line, "@RSYNCD: AUTHREQD ")
			if _, err := fmt.Fprintf(conn, "%s %s\n", b.authUser(), rsyncAuthHash(b.pass, challenge)); err != nil {
				return false, err
			}
		case line == "@RSYNCD: OK":
			for _, arg := range client.ServerCommandOptions(remotePath) {
				if _, err := fmt.Fprintf(conn, "%s\n", arg); err != nil {
					return false, err
				}
			}
			_, err := io.WriteString(conn, "\n")
			return false, err
		case line == "@RSYNCD: EXIT":
			return true, nil
		case strings.HasPrefix(line, "@ERROR"):
			return false, errors.New(line)
		}
		// Anything else is the message of the day.
	}
}

// readLine returns one line without its terminator; ctx cancellation reaches
// it through the connection being closed.
func readLine(rd *bufio.Reader) (string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// daemonProtocol parses the version out of "@RSYNCD: 31.0 sha512 ..." or
// "@RSYNCD: 27".
func daemonProtocol(greeting string) (int, error) {
	rest, ok := strings.CutPrefix(greeting, "@RSYNCD: ")
	if !ok {
		return 0, fmt.Errorf("invalid daemon greeting %q", greeting)
	}
	ver, _, _ := strings.Cut(rest, " ")
	major, _, _ := strings.Cut(ver, ".")
	v, err := strconv.Atoi(major)
	if err != nil {
		return 0, fmt.Errorf("invalid daemon greeting %q", greeting)
	}
	return v, nil
}

// authUser is the name sent to the daemon: the URL user, else what rsync
// itself would pick.
func (b *RsyncBackend) authUser() string {
	if b.user != "" {
		return b.user
	}
	if u := os.Getenv("RSYNC_USERNAME"); u != "" {
		return u
	}
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "nobody"
}

// rsyncAuthHash answers a daemon challenge the way protocol < 30 clients do:
// MD4 over a zero seed, the password and the challenge, base64 unpadded.
func rsyncAuthHash(password, challenge string) string {
	h := md4.New()
	h.Write([]byte{0, 0, 0, 0})
	h.Write([]byte(password))
	h.Write([]byte(challenge))
	return base64.RawStdEncoding.EncodeToString(h.Sum(nil))
}
