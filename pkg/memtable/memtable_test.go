package memtable_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"mini-lsm/pkg/memtable"
	"mini-lsm/pkg/test"
)

func TestMemtable(t *testing.T) {
	tb := memtable.NewTable()
	for i := uint64(0); i < 100; i++ {
		tb.Put(test.KeyOf(i), test.ValueOf(i))
	}
	iter := tb.Scan(test.KeyOf(10), test.KeyOf(20))
	for i := uint64(10); i <= 20; i++ {
		expectKey := test.KeyOf(i)
		expectValue := test.ValueOf(i)
		assert.True(t, iter.IsValid())
		assert.Equalf(t, expectKey, iter.Key(), "expect key %s, actual key: %s", expectKey, iter.Key())
		assert.Equalf(t, expectValue, iter.Value(), "expect key %s, actual key: %s", expectValue, iter.Value())
		iter.Next()
	}
}
