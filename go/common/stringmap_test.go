package common

import "testing"

func TestStringMapPreservesInsertionOrder(t *testing.T) {
	m := NewStringMap()
	m.Put("TAGS", "TagA")
	m.Put("KEYS", "k1")
	m.Put("UNIQ_KEY", "u1")
	wantKeys := []string{"TAGS", "KEYS", "UNIQ_KEY"}
	wantVals := []string{"TagA", "k1", "u1"}
	var gotKeys, gotVals []string
	m.Range(func(k, v string) {
		gotKeys = append(gotKeys, k)
		gotVals = append(gotVals, v)
	})
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] || gotVals[i] != wantVals[i] {
			t.Fatalf("entry %d: got %s=%s want %s=%s", i, gotKeys[i], gotVals[i], wantKeys[i], wantVals[i])
		}
	}
}

func TestStringMapPutUpdateKeepsPosition(t *testing.T) {
	m := NewStringMap()
	m.Put("A", "1")
	m.Put("B", "2")
	m.Put("A", "3")
	if keys := m.Keys(); len(keys) != 2 || keys[0] != "A" || keys[1] != "B" {
		t.Fatalf("keys after update: %v", keys)
	}
	if v, ok := m.Get("A"); !ok || v != "3" {
		t.Fatalf("A = %q %v", v, ok)
	}
}

func TestStringMapGetRemove(t *testing.T) {
	m := NewStringMap()
	if _, ok := m.Get("missing"); ok {
		t.Fatal("missing key must not be found")
	}
	if m.ContainsKey("missing") {
		t.Fatal("ContainsKey on missing")
	}
	m.Put("A", "1")
	m.Put("B", "2")
	m.Put("C", "3")
	m.Remove("B")
	if m.ContainsKey("B") {
		t.Fatal("B must be gone")
	}
	if keys := m.Keys(); len(keys) != 2 || keys[0] != "A" || keys[1] != "C" {
		t.Fatalf("keys after remove: %v", keys)
	}
	m.Remove("missing")
	if m.Len() != 2 {
		t.Fatalf("len %d", m.Len())
	}
}

func TestStringMapLenEmptyAndNilSafety(t *testing.T) {
	m := NewStringMap()
	if !m.IsEmpty() || m.Len() != 0 {
		t.Fatal("fresh map must be empty")
	}
	m.Put("A", "1")
	if m.IsEmpty() || m.Len() != 1 {
		t.Fatal("one entry expected")
	}
	var nilMap *StringMap
	if nilMap.Len() != 0 || !nilMap.IsEmpty() || nilMap.ContainsKey("x") {
		t.Fatal("nil receiver must be inert")
	}
	if v, ok := nilMap.Get("x"); ok || v != "" {
		t.Fatal("nil Get must be zero")
	}
	nilMap.Range(func(k, v string) { t.Fatal("nil Range must not call f") })
	nilMap.Remove("x")
	clone := nilMap.Clone()
	if clone.Len() != 0 {
		t.Fatal("nil clone must be empty")
	}
}

func TestStringMapCloneIsIndependent(t *testing.T) {
	m := NewStringMap()
	m.Put("A", "1")
	clone := m.Clone()
	clone.Put("B", "2")
	clone.Put("A", "9")
	if v, _ := m.Get("A"); v != "1" {
		t.Fatalf("original A mutated: %q", v)
	}
	if m.ContainsKey("B") {
		t.Fatal("original gained B")
	}
	if !m.Equal(m.Clone()) {
		t.Fatal("map must equal its clone")
	}
}

func TestStringMapEqualIsSetSemantics(t *testing.T) {
	a := NewStringMap()
	a.Put("TAGS", "TagA")
	a.Put("KEYS", "k1")
	b := NewStringMap()
	b.Put("KEYS", "k1")
	b.Put("TAGS", "TagA")
	if !a.Equal(b) || !b.Equal(a) {
		t.Fatal("same entries in different order must be equal")
	}
	b.Put("TAGS", "TagB")
	if a.Equal(b) {
		t.Fatal("value mismatch must not be equal")
	}
	b.Put("TAGS", "TagA")
	b.Put("EXTRA", "x")
	if a.Equal(b) {
		t.Fatal("size mismatch must not be equal")
	}
}
