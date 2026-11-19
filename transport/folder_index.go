package transport

import (
	"strings"
	"sync"
)

// folderIndex finds the paths kept under a folder by walking those alone.
// For each folder it holds the paths right in it that are kept, or have one
// kept under them. A delete looked at every path kept, under a lock: 100k
// deletes with 1M folders kept took 10^11 steps (W-M8). A path is under p
// when it starts with p and a slash, as strings.HasPrefix tells, so "." and
// ".." in a path are no folders of their own here.
//
// A path dropped some other way stays in the index until a cut that goes
// past it: the cut hands it on for nothing.
type folderIndex map[string]map[string]struct{}

// indexParent is the path p is right in: p up to its last slash, "" for a
// path with none.
func indexParent(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return ""
}

// add links p to the base, through each folder above it that is not
// linked yet.
func (x folderIndex) add(p string) {
	for p != "" {
		up := indexParent(p)
		kids := x[up]
		if kids == nil {
			kids = make(map[string]struct{})
			x[up] = kids
		}
		if _, ok := kids[p]; ok {
			return
		}
		kids[p] = struct{}{}
		p = up
	}
}

// cut hands each p and every path added under it, and forgets them.
func (x folderIndex) cut(p string, each func(string)) {
	if p != "" {
		delete(x[indexParent(p)], p)
	}
	todo := []string{p}
	for len(todo) > 0 {
		d := todo[len(todo)-1]
		todo = todo[:len(todo)-1]
		each(d)
		for k := range x[d] {
			todo = append(todo, k)
		}
		delete(x, d)
	}
}

// dirMemo is a set of folders that forgets a folder with those under it by
// walking those alone (folderIndex). The zero value is empty and ready.
type dirMemo struct {
	mu    sync.Mutex
	has   map[string]struct{}
	index folderIndex
}

func (m *dirMemo) know(d string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.has == nil {
		m.has, m.index = make(map[string]struct{}), make(folderIndex)
	}
	m.has[d] = struct{}{}
	m.index.add(d)
}

func (m *dirMemo) knows(d string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.has[d]
	return ok
}

// forget drops d alone.
func (m *dirMemo) forget(d string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.has, d)
}

// forgetTree drops d and every folder under it.
func (m *dirMemo) forgetTree(d string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.index.cut(d, func(k string) { delete(m.has, k) })
}
