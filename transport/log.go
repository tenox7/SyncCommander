package transport

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Log is the global remote-operation log shared by all transport backends.
var Log = &RemoteLog{}

// logCap bounds the retained lines. A million-file copy writes two lines per
// file, so the log keeps the most recent window rather than growing forever;
// the counters keep counting past it.
const logCap = 100000

// RemoteLog keeps a bounded window of log lines plus a parallel window of
// error lines for the errors-only view, so neither view has to scan or copy
// the whole log to render one screen.
type RemoteLog struct {
	mu             sync.Mutex
	lines          []string
	errLines       []string
	errCount       int
	retryCount     int
	recoveredCount int
	failedCount    int
	fatalCount     int
}

// Dir tags a log line: the way data moved, or the kind of failure.
type Dir int

const (
	DirOut   Dir = iota // a command or upload sent to the remote
	DirIn               // a reply or download received
	DirErr              // an operation failed; Retry may still recover it
	DirRetry            // Retry is about to try again
	DirRec              // an operation recovered after retries
	DirFail             // Retry gave up
	DirFatal            // the whole run is aborting
)

func (d Dir) String() string {
	return [...]string{">>>", "<<<", "ERR", "RETRY", "REC", "FAIL", "FATAL"}[d]
}

func (l *RemoteLog) Add(proto string, dir Dir, msg string) {
	if dir == DirErr && (strings.Contains(msg, "context canceled") || msg == "EOF") {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := time.Now().Format("15:04:05.000")
	isErr := dir == DirErr || dir == DirFail || dir == DirFatal
	for _, line := range strings.Split(strings.TrimRight(msg, "\n"), "\n") {
		line = fmt.Sprintf("%s %s %s %s", ts, proto, dir, line)
		l.lines = appendBounded(l.lines, line)
		if isErr {
			l.errLines = appendBounded(l.errLines, line)
		}
	}
	switch dir {
	case DirErr:
		l.errCount++
	case DirRetry:
		l.retryCount++
	case DirRec:
		l.recoveredCount++
	case DirFail:
		l.failedCount++
	case DirFatal:
		l.fatalCount++
	}
}

// appendBounded appends and, once the slice holds twice the cap, keeps only
// the newest cap lines in a fresh backing array so the old one is freed.
func appendBounded(lines []string, line string) []string {
	lines = append(lines, line)
	if len(lines) >= 2*logCap {
		lines = append(make([]string, 0, 2*logCap), lines[len(lines)-logCap:]...)
	}
	return lines
}

func (l *RemoteLog) ErrCount() int       { return l.count(&l.errCount) }
func (l *RemoteLog) RetryCount() int     { return l.count(&l.retryCount) }
func (l *RemoteLog) RecoveredCount() int { return l.count(&l.recoveredCount) }
func (l *RemoteLog) FailedCount() int    { return l.count(&l.failedCount) }
func (l *RemoteLog) FatalCount() int     { return l.count(&l.fatalCount) }

func (l *RemoteLog) count(p *int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return *p
}

// Lines copies every retained line; views should use Len and Slice.
func (l *RemoteLog) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// Len and ErrLen count the retained lines of each view.
func (l *RemoteLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

func (l *RemoteLog) ErrLen() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.errLines)
}

// Slice and ErrSlice copy the retained lines in [from, to), clamped.
func (l *RemoteLog) Slice(from, to int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return window(l.lines, from, to)
}

func (l *RemoteLog) ErrSlice(from, to int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return window(l.errLines, from, to)
}

func window(lines []string, from, to int) []string {
	from, to = max(from, 0), min(to, len(lines))
	if from >= to {
		return nil
	}
	return append([]string(nil), lines[from:to]...)
}
