package transport

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Log is the global remote-operation log shared by all transport backends.
var Log = &RemoteLog{}

type RemoteLog struct {
	mu       sync.Mutex
	lines    []string
	errCount int
}

func (l *RemoteLog) Add(proto, direction, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := time.Now().Format("15:04:05.000")
	for _, line := range strings.Split(strings.TrimRight(msg, "\n"), "\n") {
		l.lines = append(l.lines, fmt.Sprintf("%s %s %s %s", ts, proto, direction, line))
	}
	if direction == "ERR" {
		l.errCount++
	}
}

func (l *RemoteLog) ErrCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.errCount
}

func (l *RemoteLog) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.lines))
	copy(out, l.lines)
	return out
}

func (l *RemoteLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}
