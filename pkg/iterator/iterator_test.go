package iterator_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"mini-lsm/pkg/iterator"
	"mini-lsm/pkg/sst"
	"mini-lsm/pkg/test"
)

type MockIterator struct {
	Data  []struct{ K, V []byte }
	Index uint64
}

func NewMockIterator(data []struct{ K, V []byte }) *MockIterator {
	return &MockIterator{Data: data, Index: 0}
}

var _ iterator.Iter = (*MockIterator)(nil)

func (m *MockIterator) Key() []byte {
	return m.Data[m.Index].K
}
func (m *MockIterator) Value() []byte {
	return m.Data[m.Index].V
}
func (m *MockIterator) IsValid() bool {
	return m.Index < uint64(len(m.Data))
}
func (m *MockIterator) Next() {
	if m.Index < uint64(len(m.Data)) {
		m.Index += 1
	}
}

func CheckIterResult(t *testing.T, iter iterator.Iter, expected []struct{ K, V []byte }) {
	for i := range expected {
		assert.True(t, iter.IsValid())
		assert.Equal(t, expected[i].K, iter.Key())
		assert.Equal(t, expected[i].V, iter.Value())
		iter.Next()
	}
	assert.False(t, iter.IsValid())
}

func TestTwoMerge1(t *testing.T) {
	i1 := NewMockIterator([]struct{ K, V []byte }{
		{[]byte("a"), []byte("1.1")},
		{[]byte("b"), []byte("2.1")},
		{[]byte("c"), []byte("3.1")},
	})
	i2 := NewMockIterator([]struct{ K, V []byte }{
		{[]byte("a"), []byte("1.2")},
		{[]byte("b"), []byte("2.2")},
		{[]byte("c"), []byte("3.3")},
		{[]byte("d"), []byte("4.2")},
	})
	CheckIterResult(t, iterator.NewTwoMerger(i1, i2), []struct{ K, V []byte }{
		{[]byte("a"), []byte("1.1")},
		{[]byte("b"), []byte("2.1")},
		{[]byte("c"), []byte("3.1")},
		{[]byte("d"), []byte("4.2")},
	})
}

func TestTwoMerge2(t *testing.T) {
	i1 := NewMockIterator([]struct{ K, V []byte }{
		{[]byte("a"), []byte("1.1")},
		{[]byte("b"), []byte("2.1")},
		{[]byte("c"), []byte("3.1")},
		{[]byte("e"), []byte("5.1")},
	})
	i2 := NewMockIterator([]struct{ K, V []byte }{
		{[]byte("a"), []byte("1.2")},
		{[]byte("b"), []byte("2.2")},
		{[]byte("c"), []byte("3.3")},
		{[]byte("d"), []byte("4.2")},
	})
	CheckIterResult(t, iterator.NewTwoMerger(i2, i1), []struct{ K, V []byte }{
		{[]byte("a"), []byte("1.2")},
		{[]byte("b"), []byte("2.2")},
		{[]byte("c"), []byte("3.3")},
		{[]byte("d"), []byte("4.2")},
		{[]byte("e"), []byte("5.1")},
	})
}

func newMockIterator() (iterator.Iter, iterator.Iter, iterator.Iter) {
	i1 := NewMockIterator([]struct{ K, V []byte }{
		{[]byte("a"), []byte("1.1")},
		{[]byte("b"), []byte("2.1")},
		{[]byte("c"), []byte("3.1")},
	})
	i2 := NewMockIterator([]struct{ K, V []byte }{
		{[]byte("a"), []byte("1.2")},
		{[]byte("b"), []byte("2.2")},
		{[]byte("c"), []byte("3.2")},
		{[]byte("d"), []byte("4.2")},
	})
	i3 := NewMockIterator([]struct{ K, V []byte }{
		{[]byte("b"), []byte("2.3")},
		{[]byte("c"), []byte("3.3")},
		{[]byte("d"), []byte("4.3")},
	})
	return i1, i2, i3
}

func TestMerge1(t *testing.T) {
	i1, i2, i3 := newMockIterator()
	CheckIterResult(t, iterator.NewMergeIterator(i1, i2, i3), []struct{ K, V []byte }{
		{[]byte("a"), []byte("1.1")},
		{[]byte("b"), []byte("2.1")},
		{[]byte("c"), []byte("3.1")},
		{[]byte("d"), []byte("4.2")},
	})
}

func TestMerge2(t *testing.T) {
	i1, i2, i3 := newMockIterator()
	CheckIterResult(t, iterator.NewMergeIterator(i3, i2, i1), []struct{ K, V []byte }{
		{[]byte("a"), []byte("1.2")},
		{[]byte("b"), []byte("2.3")},
		{[]byte("c"), []byte("3.3")},
		{[]byte("d"), []byte("4.3")},
	})
}

func TestMergeTwo(t *testing.T) {
	dir := t.TempDir()
	sb := sst.NewTableBuilder(4096)
	sb.Add("a", "1.1")
	sb.Add("b", "1.2")
	sb.Add("c", "1.3")
	sb.Add("f", "1.5")
	st, err := sb.Build(0, &sync.Map{}, filepath.Join(dir, "1.sst"))
	assert.Nil(t, err)
	defer st.Close()

	mockIter := NewMockIterator([]struct{ K, V []byte }{
		{[]byte("a"), []byte("1.2")},
		{[]byte("b"), []byte("2.3")},
		{[]byte("c"), []byte("3.3")},
		{[]byte("d"), []byte("4.3")},
	})

	CheckIterResult(t, iterator.NewTwoMerger(sst.NewIterAndSeekToFirst(st), mockIter), []struct{ K, V []byte }{
		{[]byte("a"), []byte("1.1")},
		{[]byte("b"), []byte("1.2")},
		{[]byte("c"), []byte("1.3")},
		{[]byte("d"), []byte("4.3")},
		{[]byte("f"), []byte("1.5")},
	})

	mockIter = NewMockIterator([]struct{ K, V []byte }{
		{[]byte("a"), []byte("1.2")},
		{[]byte("b"), []byte("2.3")},
		{[]byte("c"), []byte("3.3")},
		{[]byte("d"), []byte("4.3")},
	})

	CheckIterResult(t, iterator.NewTwoMerger(mockIter, sst.NewIterAndSeekToFirst(st)), []struct{ K, V []byte }{
		{[]byte("a"), []byte("1.2")},
		{[]byte("b"), []byte("2.3")},
		{[]byte("c"), []byte("3.3")},
		{[]byte("d"), []byte("4.3")},
		{[]byte("f"), []byte("1.5")},
	})

	pairs := test.NewKeyValuePair(500)
	ssta, _, _ := test.GenerateSST(t.TempDir, pairs)
	defer ssta.Close()
	sb = sst.NewTableBuilder(4096)
	sb.AddByte(test.KeyOf(128), test.ValueOf(0))
	sstb, err := sb.Build(0, &sync.Map{}, filepath.Join(dir, "2.sst"))
	assert.Nil(t, err)
	defer sstb.Close()
	var result = []struct{ K, V []byte }{}
	for i := uint64(0); i < 500; i++ {
		if i == 128 {
			result = append(result, struct {
				K []byte
				V []byte
			}{
				K: test.KeyOf(i),
				V: test.ValueOf(0),
			})
		} else {
			result = append(result, struct {
				K []byte
				V []byte
			}{
				K: test.KeyOf(i),
				V: test.ValueOf(i),
			})
		}
	}
	CheckIterResult(t, iterator.NewTwoMerger(sst.NewIterAndSeekToFirst(sstb), sst.NewIterAndSeekToFirst(ssta)), result)
}

func TestMergeThree(t *testing.T) {
	pairs := test.NewKeyValuePair(500)
	ssta, _, _ := test.GenerateSST(t.TempDir, pairs)

	sb := sst.NewTableBuilder(4096)
	sb.AddByte(test.KeyOf(128), test.ValueOf(0))
	sstb, err := sb.Build(0, &sync.Map{}, filepath.Join(t.TempDir(), "1.sst"))
	assert.Nil(t, err)
	defer sstb.Close()

	sb = sst.NewTableBuilder(4096)
	sb.AddByte(test.KeyOf(127), test.ValueOf(0))
	sstc, err := sb.Build(0, &sync.Map{}, filepath.Join(t.TempDir(), "2.sst"))
	assert.Nil(t, err)
	defer sstc.Close()
	var result = []struct{ K, V []byte }{}
	for i := uint64(0); i < 500; i++ {
		if i == 128 || i == 127 {
			result = append(result, struct {
				K []byte
				V []byte
			}{
				K: test.KeyOf(i),
				V: test.ValueOf(0),
			})
		} else {
			result = append(result, struct {
				K []byte
				V []byte
			}{
				K: test.KeyOf(i),
				V: test.ValueOf(i),
			})
		}
	}
	CheckIterResult(t, iterator.NewMergeIterator(
		sst.NewIterAndSeekToFirst(sstc),
		sst.NewIterAndSeekToFirst(sstb),
		sst.NewIterAndSeekToFirst(ssta)), result)
}
