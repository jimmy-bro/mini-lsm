package lsm

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"mini-lsm/pkg/utils"

	"mini-lsm/pkg/block"
	"mini-lsm/pkg/iterator"
	"mini-lsm/pkg/memtable"
	"mini-lsm/pkg/sst"
)

type StorageInner struct {
	// mu is rw lock, rLocker should be lock on every action not modified the following struct
	// wLocker should be lock on every action modified the following struct
	mu sync.RWMutex

	memtKeyCount int64
	memtSize     int64
	memt         *memtable.Table

	immMemt    []*memtable.Table
	l0SSTables []*sst.Table
	levels     [][]*sst.Table

	nextSSTID  uint32
	path       string
	blockCache *sync.Map
}

func (si *StorageInner) Get(key []byte) []byte {
	si.mu.RLock()
	defer si.mu.RUnlock()
	val := si.memt.Get(key)
	if val != nil {
		return val
	}
	for _, mt := range si.immMemt {
		if val := mt.Get(key); val != nil {
			return val
		}
	}
	iterators := make([]iterator.Iter, 0, len(si.l0SSTables))
	for t := range si.l0SSTables {
		iterators = append(iterators, sst.NewIterAndSeekToKey(si.l0SSTables[t], key))
	}
	iter := iterator.NewMergeIterator(iterators...)
	if iter.IsValid() && bytes.Equal(iter.Key(), key) {
		return iter.Value()
	}
	return nil
}

func (si *StorageInner) Put(key, value []byte) {
	utils.Assert(len(value) != 0, "value cannot be empty")
	utils.Assert(len(key) != 0, "key cannot be empty")

	estimateSize := block.SizeOfUint16*2 + uint16(len(key)) + uint16(len(value)) + block.SizeOfUint16
	si.mu.RLock()
	si.memt.Put(key, value)
	atomic.AddInt64(&si.memtKeyCount, 1)
	atomic.AddInt64(&si.memtSize, int64(estimateSize))
	si.mu.RUnlock()
}

func (si *StorageInner) Delete(key []byte) {
	utils.Assert(len(key) != 0, "key cannot be empty")
	si.mu.RLock()
	si.memt.Put(key, nil)
	si.mu.RUnlock()
}

func (si *StorageInner) Scan(lower, upper []byte) iterator.Iter {
	si.mu.RLock()
	defer si.mu.RUnlock()
	var iterators = make([]iterator.Iter, 0, 1+len(si.immMemt)+len(si.l0SSTables))
	iterators = append(iterators, si.memt.Scan(lower, upper))
	for _, mt := range si.immMemt {
		iterators = append(iterators, mt.Scan(lower, upper))
	}
	for t := range si.l0SSTables {
		iterators = append(iterators, sst.NewIterAndSeekToKey(si.l0SSTables[t], lower))
	}
	return iterator.NewMergeIterator(iterators...)
}

func (si *StorageInner) checkIfNewMemTableShouldBeCreate() bool {
	return atomic.LoadInt64(&si.memtKeyCount) > 1000 || atomic.LoadInt64(&si.memtSize) > 4096*10
}

func (si *StorageInner) newMemTable() {
	si.mu.Lock()
	si.memt, si.immMemt = memtable.NewTable(), append(si.immMemt, si.memt)
	atomic.SwapInt64(&si.memtKeyCount, 0)
	atomic.SwapInt64(&si.memtSize, 0)
	si.mu.Unlock()
}

func (si *StorageInner) sstPath(id uint32) string {
	return filepath.Join(si.path, fmt.Sprintf("%d.sst", id))
}

func (si *StorageInner) checkIfImMemTableShouldFlushToSST() bool {
	return len(si.immMemt) > 0
}

func (si *StorageInner) sinkImMemTableToSST() error {
	sstID := si.nextSSTID
	si.mu.Lock()
	defer si.mu.Unlock()

	flushMemTable := si.immMemt[len(si.immMemt)-1]
	builder := sst.NewTableBuilder(4096)
	flushMemTable.Flush(builder)

	sstTable, err := builder.Build(sstID, si.blockCache, si.sstPath(sstID))
	if err != nil {
		return err
	}

	si.immMemt = si.immMemt[:len(si.immMemt)-1]
	si.l0SSTables = append([]*sst.Table{sstTable}, si.l0SSTables...)
	si.nextSSTID += 1
	return nil
}

func (si *StorageInner) checkIfSSTShouldBeCompact() bool {
	return len(si.l0SSTables) >= 2
}
func (si *StorageInner) compactSSTs() {
	log.Printf("compact with l0SSTables: %d", len(si.l0SSTables))
	if len(si.l0SSTables) >= 2 {
		si.mu.RLock()
		l0SSTableLength := len(si.l0SSTables)
		sn := si.l0SSTables[l0SSTableLength-1]
		snID := sn.SSTID()
		snm1 := si.l0SSTables[l0SSTableLength-2]
		snm1ID := snm1.SSTID()
		si.mu.RUnlock()

		snIter := sst.NewIterAndSeekToFirst(sn)
		snm1Iter := sst.NewIterAndSeekToFirst(snm1)
		mergeIter := iterator.NewTwoMerger(snm1Iter, snIter)
		builder := sst.NewTableBuilder(4096)
		for mergeIter.IsValid() {
			builder.AddByte(mergeIter.Key(), mergeIter.Value())
			mergeIter.Next()
		}
		sstID := si.nextSSTID
		sstTable, err := builder.Build(sstID, si.blockCache, si.sstPath(sstID))
		if err != nil {
			log.Printf("sstable build fail: %s", err)
			return
		}
		si.nextSSTID += 1
		defer func() {
			snm1.Close()
			sn.Close()
			os.Remove(si.sstPath(snID))
			os.Remove(si.sstPath(snm1ID))
		}()
		si.mu.Lock()
		if si.l0SSTables[l0SSTableLength-1].SSTID() == snID &&
			si.l0SSTables[l0SSTableLength-2].SSTID() == snm1ID {
			si.l0SSTables = append(si.l0SSTables[:l0SSTableLength-2], sstTable)
		}
		si.mu.Unlock()
	}
}

func (si *StorageInner) internalLoopTask() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		if si.checkIfNewMemTableShouldBeCreate() {
			logrus.Infoln("create new memtable")
			si.newMemTable()
		}

		if si.checkIfImMemTableShouldFlushToSST() {
			logrus.Infoln("start to sink immutable memtable to sst")
			err := si.sinkImMemTableToSST()
			if err != nil {
				logrus.WithError(err).Errorln("sinkImMemTableToSST error")
			}
		}

		if si.checkIfSSTShouldBeCompact() {
			// now compact has problem
			si.compactSSTs()
		}
	}
}

func NewStorageInner(path string) *StorageInner {
	si := &StorageInner{
		memt:       memtable.NewTable(),
		immMemt:    make([]*memtable.Table, 0),
		l0SSTables: make([]*sst.Table, 0),
		levels:     make([][]*sst.Table, 0),
		nextSSTID:  1,
		path:       path,
		blockCache: &sync.Map{},
	}
	go si.internalLoopTask()
	return si
}

type Storage struct {
	// inner StorageInner implement a lsm storage
	*StorageInner
}

func NewStorage(path string) *Storage {
	return &Storage{
		StorageInner: NewStorageInner(path),
	}
}
