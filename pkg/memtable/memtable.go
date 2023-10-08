package memtable

import (
	"bytes"
	"sync"

	"github.com/huandu/skiplist"

	"mini-lsm/pkg/sst"
)

type Table struct {
	mu sync.RWMutex
	m  *skiplist.SkipList
}

func NewTable() *Table {
	return &Table{m: skiplist.New(skiplist.Bytes)}
}

func (t *Table) Get(key []byte) []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ele, ok := t.m.GetValue(key)
	if !ok {
		return nil
	}
	return inlineDeepCopy(ele.([]byte))
}

func inlineDeepCopy(in []byte) (out []byte) {
	out = make([]byte, len(in))
	copy(out, in)
	return out
}

func (t *Table) Put(key, value []byte) {
	t.mu.Lock()
	t.m.Set(inlineDeepCopy(key), inlineDeepCopy(value))
	t.mu.Unlock()
}

func (t *Table) Scan(lower, upper []byte) *Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()
	head := t.m.Find(lower)
	return &Iterator{ele: head, end: upper}
}

func (t *Table) Flush(builder *sst.TableBuilder) {
	head := t.m.Front()
	if head == nil {
		return
	}
	for {
		builder.AddByte(head.Key().([]byte), head.Value.([]byte))
		next := head.Next()
		if next == nil {
			break
		}
		head = next
	}
}

type Iterator struct {
	ele *skiplist.Element
	end []byte
}

func (m *Iterator) Value() []byte {
	return inlineDeepCopy(m.ele.Value.([]byte))
}

func (m *Iterator) Key() []byte {
	if m.ele == nil {
		return nil
	}
	return inlineDeepCopy(m.ele.Key().([]byte))
}

func (m *Iterator) IsValid() bool {
	return m.ele != nil && len(m.ele.Key().([]byte)) != 0
}

func (m *Iterator) Next() {
	m.ele = m.ele.Next()
	if m.ele != nil && bytes.Compare(m.ele.Key().([]byte), m.end) == 1 {
		m.ele = nil
		return
	}
}
