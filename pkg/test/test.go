package test

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"unsafe"

	"mini-lsm/pkg/sst"
)

func s2b(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	// // nolint:govet // unsafe for transfer string to []byte
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func BigKeyOf(idx uint64) []byte {
	return s2b(fmt.Sprintf("big_key_%0*d", 16, idx))
}

func BigValueOf(idx uint64) []byte {
	return s2b(fmt.Sprintf("big_value_%0*d", 16, idx))
}

func KeyOf(idx uint64) []byte {
	return s2b(fmt.Sprintf("key_%0*d", 8, idx))
}

func ValueOf(idx uint64) []byte {
	return s2b(fmt.Sprintf("value_%0*d", 8, idx))
}

const (
	GenerateBlockSize = 4096
)

type Pair struct {
	Key, Value []byte
}

func NewKeyValuePair(keyCount uint64) []Pair {
	var out = make([]Pair, 0, keyCount)
	for i := uint64(0); i < keyCount; i++ {
		out = append(out, Pair{
			Key:   KeyOf(i),
			Value: ValueOf(i),
		})
	}
	return out
}

func GenerateSST(tempdirFn func() string, keyValuePairs []Pair) (*sst.Table, string, error) {
	tb := sst.NewTableBuilder(GenerateBlockSize)
	for i := range keyValuePairs {
		tb.AddByte(keyValuePairs[i].Key, keyValuePairs[i].Value)
	}
	tempdir := tempdirFn()
	fp := filepath.Join(tempdir, "1.sst")
	sstable, err := tb.Build(1, &sync.Map{}, fp)
	return sstable, fp, err
}
