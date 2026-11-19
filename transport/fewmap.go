package transport

import "slices"

// fewMax is how many entries a fewMap holds in its list.
const fewMax = 32

// fewMap is a map by name that holds up to fewMax entries in a list, with
// a quarter more room at the most, and more in a Go map. A Go map holds 8
// slots at the least, and up to twice the entries it holds: one for each
// folder took some 750 bytes for the sums of a single file (W-M8). The
// zero value is empty and ready.
type fewMap[V any] struct {
	few  []fewEntry[V]
	many map[string]V
}

type fewEntry[V any] struct {
	name string
	v    V
}

func (m *fewMap[V]) get(name string) (V, bool) {
	if m.many != nil {
		v, ok := m.many[name]
		return v, ok
	}
	for i := range m.few {
		if m.few[i].name == name {
			return m.few[i].v, true
		}
	}
	var zero V
	return zero, false
}

func (m *fewMap[V]) set(name string, v V) {
	if m.many != nil {
		m.many[name] = v
		return
	}
	for i := range m.few {
		if m.few[i].name == name {
			m.few[i].v = v
			return
		}
	}
	if n := len(m.few); n < fewMax {
		if n == cap(m.few) {
			few := make([]fewEntry[V], n, min(n+1+n/4, fewMax))
			copy(few, m.few)
			m.few = few
		}
		m.few = append(m.few, fewEntry[V]{name, v})
		return
	}
	m.many = make(map[string]V, fewMax+1)
	for _, e := range m.few {
		m.many[e.name] = e.v
	}
	m.many[name] = v
	m.few = nil
}

func (m *fewMap[V]) del(name string) {
	if m.many != nil {
		delete(m.many, name)
		return
	}
	if i := slices.IndexFunc(m.few, func(e fewEntry[V]) bool { return e.name == name }); i >= 0 {
		m.few = slices.Delete(m.few, i, i+1)
	}
}

func (m *fewMap[V]) len() int {
	if m.many != nil {
		return len(m.many)
	}
	return len(m.few)
}

// each hands each entry; fn must not change m.
func (m *fewMap[V]) each(fn func(name string, v V)) {
	if m.many != nil {
		for k, v := range m.many {
			fn(k, v)
		}
		return
	}
	for _, e := range m.few {
		fn(e.name, e.v)
	}
}
