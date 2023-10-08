package sst

import (
	"mini-lsm/pkg/block"
	"mini-lsm/pkg/iterator"
)

type Iter struct {
	table   *Table
	blkIter *block.Iter
	blkIdx  uint32
}

var _ iterator.Iter = &Iter{}

func NewIterAndSeekToFirst(table *Table) *Iter {
	blkIter := block.NewBlockIterAndSeekToFirst(table.ReadBlockCached(0))
	return &Iter{table: table, blkIter: blkIter, blkIdx: 0}
}

func NewIterAndSeekToKey(table *Table, key []byte) *Iter {
	blkIdx, iter := seekToKey(table, key)
	return &Iter{table: table, blkIter: iter, blkIdx: blkIdx}
}

func (i *Iter) SeekToFirst() {
	i.blkIdx = 0
	i.blkIter = block.NewBlockIterAndSeekToFirst(i.table.ReadBlockCached(0))
}

func seekToKey(t *Table, key []byte) (uint32, *block.Iter) {
	blkIdx := t.FindBlockIdx(key)
	blkIter := block.NewBlockIterAndSeekToKey(t.ReadBlockCached(blkIdx), key)
	if !blkIter.IsValid() {
		blkIdx++
		if blkIdx < t.Len() {
			blkIter = block.NewBlockIterAndSeekToFirst(t.ReadBlockCached(blkIdx))
		}
	}
	return blkIdx, blkIter
}

func (i *Iter) SeekToKey(key []byte) {
	i.blkIdx, i.blkIter = seekToKey(i.table, key)
}

func (i *Iter) Key() []byte {
	return i.blkIter.Key()
}

func (i *Iter) Value() []byte {
	return i.blkIter.Value()
}
func (i *Iter) IsValid() bool {
	return i.blkIter.IsValid()
}
func (i *Iter) Next() {
	i.blkIter.Next()
	if !i.blkIter.IsValid() {
		i.blkIdx++
		if i.blkIdx < i.table.Len() {
			i.blkIter = block.NewBlockIterAndSeekToFirst(i.table.ReadBlockCached(i.blkIdx))
		}
	}
}
