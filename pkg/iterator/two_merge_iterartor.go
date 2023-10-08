package iterator

import (
	"bytes"
)

// TwoMergeIterator hold two Iter, it can get key-value pairs one by one
// If key exists in both A and B, it returns key-values in A first
type TwoMergeIterator struct {
	A       Iter
	B       Iter
	chooseA bool
}

func NewTwoMerger(a, b Iter) *TwoMergeIterator {
	iter := &TwoMergeIterator{A: a, B: b}
	iter.SkipB()
	iter.chooseA = iter.ChooseA()
	return iter
}

func (t *TwoMergeIterator) ChooseA() bool {
	if !t.A.IsValid() {
		return false
	}
	if !t.B.IsValid() {
		return true
	}
	return bytes.Compare(t.A.Key(), t.B.Key()) == -1
}

func (t *TwoMergeIterator) SkipB() {
	if t.A.IsValid() {
		for t.B.IsValid() && bytes.Equal(t.B.Key(), t.A.Key()) {
			t.B.Next()
		}
	}
}

var _ Iter = (*TwoMergeIterator)(nil)

func (t *TwoMergeIterator) Key() []byte {
	if t.chooseA {
		return t.A.Key()
	}
	return t.B.Key()
}

func (t *TwoMergeIterator) Value() []byte {
	if t.chooseA {
		return t.A.Value()
	}
	return t.B.Value()
}

func (t *TwoMergeIterator) IsValid() bool {
	if t.chooseA {
		return t.A.IsValid()
	}
	return t.B.IsValid()
}

func (t *TwoMergeIterator) Next() {
	if t.chooseA {
		t.A.Next()
	} else {
		t.B.Next()
	}
	t.SkipB()
	t.chooseA = t.ChooseA()
}
