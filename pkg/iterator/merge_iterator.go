package iterator

import (
	"bytes"

	"mini-lsm/pkg/utils"
)

// MergeIterator can merge many iterators to one
// all different key will remain
// if there are same keys, will take iter which index is small
type MergeIterator struct {
	iterators []Iter
	current   int
}

// NewMergeIterator receives one or more iterators
// return a MergeIterator
func NewMergeIterator(in ...Iter) *MergeIterator {
	if len(in) == 0 {
		return &MergeIterator{iterators: in, current: -1}
	}

	iterators := make([]Iter, 0)
	for i := range in {
		if !in[i].IsValid() {
			continue
		}
		iterators = append(iterators, in[i])
	}
	if len(iterators) == 0 {
		return &MergeIterator{iterators: iterators, current: -1}
	}
	return &MergeIterator{iterators: iterators, current: findMinimalIter(iterators)}
}

func findMinimalIter(iterators []Iter) int {
	// every iter is valid, we want to find the smallest key
	min := 0
	for i := 1; i < len(iterators); i++ {
		if bytes.Compare(iterators[min].Key(), iterators[i].Key()) == 1 {
			min = i
		}
	}
	return min
}

func (m *MergeIterator) Key() []byte {
	utils.Assertf(m.current < len(m.iterators),
		"current iterator idx %d should less than length of iterators %d", m.current, len(m.iterators))
	return m.iterators[m.current].Key()
}

func (m *MergeIterator) Value() []byte {
	utils.Assertf(m.current < len(m.iterators),
		"current iterator idx %d should less than length of iterators %d", m.current, len(m.iterators))
	return m.iterators[m.current].Value()
}

func (m *MergeIterator) IsValid() bool {
	return m.current >= 0 &&
		m.current < len(m.iterators) &&
		m.iterators[m.current].IsValid()
}

// Next should skip all same key in every ite
func (m *MergeIterator) Next() {
	currentKey := make([]byte, len(m.iterators[m.current].Key()))
	copy(currentKey, m.iterators[m.current].Key())

	// 1. move current iter to next
	m.iterators[m.current].Next()
	if !m.iterators[m.current].IsValid() {
		m.iterators = append(m.iterators[:m.current], m.iterators[m.current+1:]...)
	}

	// 2. remove all dup keys
	for i := 0; i < len(m.iterators); i++ {
		for m.iterators[i].IsValid() && bytes.Equal(m.iterators[i].Key(), currentKey) {
			m.iterators[i].Next()
		}
	}

	// 3. remove all invalid iter
	i := len(m.iterators) - 1
	for i >= 0 {
		if !m.iterators[i].IsValid() {
			m.iterators = append(m.iterators[:i], m.iterators[i+1:]...)
		}
		i--
	}

	m.current = findMinimalIter(m.iterators)
}
