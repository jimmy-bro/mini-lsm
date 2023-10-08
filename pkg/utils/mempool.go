package utils

import (
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
)

// GlobalPool just for test
// nolint:gochecknoglobals // this may not be used in production mode, just for test and benchmark
var GlobalPool SlicePool

// nolint:gochecknoinits // this may not be used in production mode, just for test and benchmark
func init() {
	GlobalPool = NewSlicePool(81920, 256)
}

type SlicePool interface {
	Get(int) []byte
	Put([]byte)
}

type slicePool struct {
	pools     []sync.Pool
	minLenExp int
	maxLenExp int
}

// ceilLog2 returns the minimum e that 2^e >= size
// eg. ceilLog2(5) = 3, ceilLog2(16) = 4
func ceilLog2(length int) int {
	return sort.Search(64, func(i int) bool {
		return 1<<uint(i) >= length
	})
}

// floorLog2 returns the maximum e that 2^e <= size
// eg. floorLog2(5) = 2, floorLog2(8) = 3
func floorLog2(length int) int {
	return sort.Search(64, func(i int) bool {
		return 1<<uint(i) > length
	}) - 1
}

func (sp *slicePool) Get(length int) []byte {
	index := ceilLog2(length) - sp.minLenExp
	if index >= len(sp.pools) {
		return nil
	}
	if index < 0 {
		index = 0
	}
	return (sp.pools[index].Get().([]byte))[:length]
}

func (sp *slicePool) Put(s []byte) {
	if s == nil {
		return
	}
	index := floorLog2(cap(s)) - sp.minLenExp
	if index >= len(sp.pools) || index < 0 {
		return
	}
	// nolint:staticcheck // data on bytes will not be copied
	sp.pools[index].Put(s)
}

func NewSlicePool(maxLen, minLen int) SlicePool {
	sp := &slicePool{
		minLenExp: floorLog2(minLen),
		maxLenExp: ceilLog2(maxLen),
	}
	logrus.WithField("minLengthExp", sp.minLenExp).WithField("maxLengthExp", sp.maxLenExp).Infoln("NewSlicePool")
	sp.pools = make([]sync.Pool, sp.maxLenExp-sp.minLenExp+1)

	for i := sp.minLenExp; i <= sp.maxLenExp; i++ {
		tempLength := 1 << uint(i)
		sp.pools[i-sp.minLenExp] = sync.Pool{
			New: func() interface{} {
				return make([]byte, tempLength)
			},
		}
	}
	return sp
}
