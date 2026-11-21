package transport

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"sc/model"
)

// connGate holds back the connections to a server that turned one away for
// having as many as it takes (model.ErrTooMany), as an rsync daemon does
// past "max connections". It counts the connections sc has open to the
// server. Once one is turned away, no more are open at a time than the
// server took then, and a connect waits for one of them to close. With 8
// parallel copies to a daemon that takes 4, 4 failed each try, and gave up
// after six (some 15 s), each with an ERR line that opened the log.
type connGate struct {
	mu    sync.Mutex
	open  int           // connections open now, or being made
	limit int           // what the server takes; 0 while it turned none away
	freed chan struct{} // closed when one closes or the limit goes
}

// serverFullWait is how long connects are tried again while the server
// turns each away with no connection of sc's own open to it: other clients
// hold them all. A var for tests.
var serverFullWait = 2 * time.Minute

// errServerFull is the end of serverFullWait. No file of a copy gets
// through, so the copy stops (GivesUp).
var errServerFull = errors.New("the server took no connection")

// closeLag is the first wait of a login turned away within the limit: a
// connection of sc's that closed a moment ago may still be counted by the
// server, as an rsync daemon counts it until the process that served it
// ends, and on a loaded host that took over a second. Each wait in a row
// doubles. A var for tests.
var closeLag = 500 * time.Millisecond

// stuckTurns is how many turns away in a row within the limit, with the
// waits of closeLag between them, show another client took a place: more
// than a closed connection of sc's still counted. The limit then comes
// down by one.
const stuckTurns = 4

// enter waits for a place: while the server's limit is known and that many
// are open, until one closes. The wait is no stall (waitTouching). waited:
// it waited for one to close.
func (g *connGate) enter(ctx context.Context) (waited bool, err error) {
	g.mu.Lock()
	for g.limit > 0 && g.open >= g.limit {
		if g.freed == nil {
			g.freed = make(chan struct{})
		}
		freed := g.freed
		g.mu.Unlock()
		if err := waitTouching(ctx, freed, 0); err != nil {
			return true, err
		}
		waited = true
		g.mu.Lock()
	}
	g.open++
	g.mu.Unlock()
	return waited, nil
}

// leave gives the place of a connection that closed.
func (g *connGate) leave() {
	g.mu.Lock()
	g.open--
	g.wakeLocked()
	g.mu.Unlock()
}

// turnedAway is leave for a connection the server turned away for having
// too many. most is the most the server said it takes, 0 when it did not
// say: the limit comes down to it at once. With stuck, stuckTurns in a row
// of one connect within the limit seen, the limit when they began, it
// comes down by one, or, while none is known, to the others sc has open:
// another client took a place. A limit another connect brought down since
// is not brought down again: 8 copies in a burst each stuck would take it
// down by 8. It never goes below one. Returns the limit, whether this
// brought it down, and how many of sc's are open.
func (g *connGate) turnedAway(stuck bool, seen, most int) (limit int, lowered bool, open int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open--
	n := 0
	switch {
	case most > 0 && (g.limit == 0 || most < g.limit):
		n = most
	case stuck && g.limit != seen:
	case stuck && g.limit == 0:
		n = max(g.open, 1)
	case stuck && g.limit > 1:
		n = g.limit - 1
	}
	if n > 0 {
		g.limit, lowered = n, true
	}
	return g.limit, lowered, g.open
}

// raise lets more connections be open again, as + asks for: a limit under n
// is forgotten, and the server shows again what it takes.
func (g *connGate) raise(n int) {
	g.mu.Lock()
	if g.limit > 0 && n > g.limit {
		g.limit = 0
		g.wakeLocked()
	}
	g.mu.Unlock()
}

// limitNow is what the server takes, 0 while none was turned away.
func (g *connGate) limitNow() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

func (g *connGate) wakeLocked() {
	if g.freed != nil {
		close(g.freed)
		g.freed = nil
	}
}

// touchEvery is how often waitTouching touches the stall guard. A var for
// tests.
var touchEvery = time.Second

// waitTouching waits for ch to close, d to pass (d > 0) or ctx to end, and
// tells the stall guard over ctx each touchEvery that the wait is no stall: the
// copy waits for a connection, not for a server that stopped answering.
func waitTouching(ctx context.Context, ch <-chan struct{}, d time.Duration) error {
	var timeout <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}
	tick := time.NewTicker(touchEvery)
	defer tick.Stop()
	for {
		Touch(ctx)
		select {
		case <-ch:
			return nil
		case <-timeout:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// ConnLimiter is a backend whose server may turn a connection away for
// having too many, and so knows how many it takes (ConnLimit).
type ConnLimiter interface{ ConnLimit() int }

// ConnLimit is how many copies at once b's server takes, as sc found when
// the server turned a connection away for having too many
// (model.ErrTooMany); 0 while it turned none away. The UI lowers its
// parallel copies to it: a copy over it waits for a connection.
func ConnLimit(b model.Backend) int {
	if l, ok := b.(ConnLimiter); ok {
		return l.ConnLimit()
	}
	return 0
}

// tooManyFTP tells an extra FTP connection the server turned away for
// having as many as it takes (ftpTooMany).
func tooManyFTP(err error) bool { return errors.Is(err, model.ErrTooMany) }

// tooManySSH tells an extra ssh connection the server turned away: one it
// refused, or hung up on before the login ended, while the primary is
// logged in, as sshd does past MaxStartups, and a firewall past the
// connections it lets one address have. A timeout is not one: it says
// nothing of how many the server takes.
func tooManySSH(err error) bool {
	err = connectFailure(err)
	var ne net.Error
	timeout := errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &ne) && ne.Timeout()
	return errors.Is(err, model.ErrTooMany) || errors.Is(err, model.ErrUnreachable) && !timeout
}
