package common

// StringMap is a string-to-string map that preserves insertion order.
//
// The 17th segment of the message format concatenates `k\x01v\x02` entries in
// insertion order, and header extFields round-trip the same way, so a plain Go
// map (random iteration order) would break byte-for-byte wire compatibility.
type StringMap struct {
	keys []string
	vals map[string]string
}

func NewStringMap() *StringMap {
	return &StringMap{vals: make(map[string]string)}
}

// Put inserts or updates; an update keeps the original insertion position.
func (m *StringMap) Put(key, value string) {
	if m.vals == nil {
		m.vals = make(map[string]string)
	}
	if _, ok := m.vals[key]; !ok {
		m.keys = append(m.keys, key)
	}
	m.vals[key] = value
}

func (m *StringMap) Get(key string) (string, bool) {
	if m == nil {
		return "", false
	}
	v, ok := m.vals[key]
	return v, ok
}

func (m *StringMap) ContainsKey(key string) bool {
	if m == nil {
		return false
	}
	_, ok := m.vals[key]
	return ok
}

func (m *StringMap) Remove(key string) {
	if m == nil {
		return
	}
	if _, ok := m.vals[key]; !ok {
		return
	}
	delete(m.vals, key)
	for i, k := range m.keys {
		if k == key {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
}

func (m *StringMap) Len() int {
	if m == nil {
		return 0
	}
	return len(m.keys)
}

func (m *StringMap) IsEmpty() bool { return m.Len() == 0 }

// Keys returns a copy of the keys in insertion order.
func (m *StringMap) Keys() []string {
	if m == nil {
		return nil
	}
	return append([]string(nil), m.keys...)
}

// Range calls f for each entry in insertion order.
func (m *StringMap) Range(f func(key, value string)) {
	if m == nil {
		return
	}
	for _, k := range m.keys {
		f(k, m.vals[k])
	}
}

func (m *StringMap) Clone() *StringMap {
	out := NewStringMap()
	if m == nil {
		return out
	}
	out.keys = append(out.keys, m.keys...)
	for k, v := range m.vals {
		out.vals[k] = v
	}
	return out
}

// Equal compares as sets (order-insensitive), like the Rust IndexMap PartialEq
// the other ports assert against.
func (m *StringMap) Equal(other *StringMap) bool {
	if m.Len() != other.Len() {
		return false
	}
	for k, v := range m.vals {
		ov, ok := other.vals[k]
		if !ok || ov != v {
			return false
		}
	}
	return true
}
