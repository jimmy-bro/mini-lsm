package sst_test

import (
	"crypto/rand"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"mini-lsm/pkg/sst"
	"mini-lsm/pkg/test"
)

func TestBuildSSTSingleKey(t *testing.T) {
	tb := sst.NewTableBuilder(16)
	tb.Add("233", "233333")
	tempdir := t.TempDir()
	_, err := tb.Build(0, &sync.Map{}, filepath.Join(tempdir, "1.sst"))
	assert.Nil(t, err)
}

func TestBuildSSTTowBlocks(t *testing.T) {
	tb := sst.NewTableBuilder(16)
	tb.Add("11", "11")
	tb.Add("22", "22")
	tb.Add("33", "33")
	tb.Add("44", "44")
	tb.Add("55", "55")
	tb.Add("66", "66")
	assert.Greater(t, tb.Len(), uint32(2))
	tempdir := t.TempDir()
	sstable, err := tb.Build(0, &sync.Map{}, filepath.Join(tempdir, "1.sst"))
	assert.Nil(t, err)
	assert.NotNil(t, sstable)
}

func TestGenerateSST(t *testing.T) {
	pairs := test.NewKeyValuePair(1000)
	st, _, err := test.GenerateSST(t.TempDir, pairs)
	assert.Nil(t, err)
	assert.Nil(t, st.Close())
}

func TestSSTDecode(t *testing.T) {
	pairs := test.NewKeyValuePair(1000)
	sstable, fp, err := test.GenerateSST(t.TempDir, pairs)
	assert.Nil(t, err)
	fd, err := os.Open(fp)
	assert.Nil(t, err)
	nsstable, err := sst.OpenTableFromFile(0, &sync.Map{}, fd)
	assert.Nil(t, err)
	assert.Equal(t, sstable.Meta(), nsstable.Meta())
	assert.Nil(t, sstable.Close())
	assert.Nil(t, nsstable.Close())
}

func TestSSTIterSeekToFirst(t *testing.T) {
	pairs := test.NewKeyValuePair(1000)
	sstable, _, err := test.GenerateSST(t.TempDir, pairs)
	assert.Nil(t, err)
	defer sstable.Close()
	iter := sst.NewIterAndSeekToFirst(sstable)
	for i := 0; i < 5; i++ {
		for j := uint64(0); j < 100; j++ {
			key := iter.Key()
			value := iter.Value()
			assert.Equalf(t, test.KeyOf(j), key, "expect key %s, actual key: %s", test.KeyOf(j), key)
			assert.Equalf(t, test.ValueOf(j), value, "expect value %s, actual value: %s", test.ValueOf(j), value)
			iter.Next()
		}
		iter.SeekToFirst()
	}
}

func TestSSTIterSeekToKey(t *testing.T) {
	pairs := test.NewKeyValuePair(1000)
	sstable, _, err := test.GenerateSST(t.TempDir, pairs)
	assert.Nil(t, err)
	defer sstable.Close()
	iter := sst.NewIterAndSeekToFirst(sstable)
	for i := 0; i < 5; i++ {
		rdInt, _ := rand.Int(rand.Reader, big.NewInt(64))
		idx := rdInt.Uint64() % 1000
		iter.SeekToKey(test.KeyOf(idx))
		key := iter.Key()
		value := iter.Value()
		assert.Equalf(t, test.KeyOf(idx), key, "expect key %s, actual key: %s", test.KeyOf(idx), key)
		assert.Equalf(t, test.ValueOf(idx), value, "expect value %s, actual value: %s", test.ValueOf(idx), value)
	}
}

func BenchmarkSSTEncode(b *testing.B) {
	pairs := test.NewKeyValuePair(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table, _, _ := test.GenerateSST(b.TempDir, pairs)
		table.Close()
	}
}

func BenchmarkSSTDecode(b *testing.B) {
	pairs := test.NewKeyValuePair(1000)
	st, fp, _ := test.GenerateSST(b.TempDir, pairs)
	st.Close()
	for i := 0; i < b.N; i++ {
		fd, _ := os.Open(fp)
		tb, _ := sst.OpenTableFromFile(0, &sync.Map{}, fd)
		tb.Close()
	}
}

func BenchmarkSSTIterSeekToFirst(b *testing.B) {
	pairs := test.NewKeyValuePair(1000)
	sstable, _, _ := test.GenerateSST(b.TempDir, pairs)
	iter := sst.NewIterAndSeekToFirst(sstable)
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < 100; j++ {
			iter.Next()
		}
	}
	sstable.Close()
}

func BenchmarkSSTIterSeekToKeyExists(b *testing.B) {
	pairs := test.NewKeyValuePair(1000)
	sstable, _, _ := test.GenerateSST(b.TempDir, pairs)
	iter := sst.NewIterAndSeekToFirst(sstable)
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < 100; j++ {
			iter.SeekToFirst()
			iter.SeekToKey(test.KeyOf(j))
		}
	}
	sstable.Close()
}

func BenchmarkSSTIterSeekToKeyNonExists(b *testing.B) {
	pairs := test.NewKeyValuePair(1000)
	sstable, _, _ := test.GenerateSST(b.TempDir, pairs)
	iter := sst.NewIterAndSeekToFirst(sstable)
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < 100; j++ {
			iter.SeekToFirst()
			iter.SeekToKey(test.KeyOf(j + 10086))
		}
	}
	sstable.Close()
}
