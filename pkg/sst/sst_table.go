package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"mini-lsm/pkg/block"
	"mini-lsm/pkg/utils"
)

var ErrReadBlockError = errors.New("read block error")

// Table is a sorted string table
type Table struct {
	// fd hold the file descriptor of the open file
	fd *os.File

	// all metas, hold block offset and first key
	metas []*block.Meta

	// metaOffsets
	metaOffsets uint32
	id          uint32

	// blockCache is a map[[2]uint32]*block.Block
	blockCache *sync.Map
}

func OpenTableFromFile(id uint32, blockCache *sync.Map, fd *os.File) (*Table, error) {
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	// read meta offset(last block.SizeOfUint32 byte)
	var rawMetaOffset [block.SizeOfUint32]byte
	n, err := fd.ReadAt(rawMetaOffset[:], int64(uint16(fi.Size())-block.SizeOfUint32))
	if err != nil {
		return nil, err
	}
	if uint16(n) != block.SizeOfUint32 {
		return nil, fmt.Errorf("misread the meta offset %d, should be %d", n, block.SizeOfUint32)
	}
	blockMetaOffset := binary.BigEndian.Uint32(rawMetaOffset[:])

	// seek to offset for reading metadata
	_, err = fd.Seek(int64(blockMetaOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	// sst: | blocks | block_metadata{offset, firstkey} | metadata_offset |
	rawMetas, err := block.DecodeBlockMetaFromReader(io.LimitReader(fd, int64(uint16(fi.Size())-block.SizeOfUint32)-int64(blockMetaOffset)))
	if err != nil {
		return nil, err
	}
	return &Table{
		fd:          fd,
		metas:       rawMetas,
		metaOffsets: blockMetaOffset,
		id:          id,
		blockCache:  blockCache,
	}, err
}

func (t *Table) Close() error {
	return t.fd.Close()
}

func (t *Table) ReadBlock(blockIdx uint32) (*block.Block, error) {
	offset := t.metas[blockIdx].Offset
	var offsetEnd uint32
	if blockIdx < uint32(len(t.metas)-1) {
		offsetEnd = t.metas[blockIdx+1].Offset
	} else {
		offsetEnd = t.metaOffsets
	}
	data := utils.GlobalPool.Get(int(offsetEnd - offset))
	defer utils.GlobalPool.Put(data)
	n, err := t.fd.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	if n != int(offsetEnd-offset) {
		return nil, ErrReadBlockError
	}
	b := &block.Block{}
	b.Decode(data)
	return b, nil
}

func (t *Table) ReadBlockCached(blockIdx uint32) *block.Block {
	key := [2]uint32{t.id, blockIdx}
	if v, ok := t.blockCache.Load(key); ok {
		return v.(*block.Block)
	}
	blk, err := t.ReadBlock(blockIdx)
	utils.Assertf(err == nil, "read block id: %d error: %s", blockIdx, err)
	t.blockCache.Store(key, blk)
	return blk
}

func (t *Table) FindBlockIdx(key []byte) uint32 {
	satSub1 := func(a uint32) uint32 {
		if a > 0 {
			return a - 1
		}
		return 0
	}
	for i := uint32(0); i < t.Len(); i++ {
		// firstKey <= key
		if bytes.Compare(t.metas[i].FirstKey, key) > 0 {
			return satSub1(i)
		}
	}
	return satSub1(t.Len())
}

func (t *Table) Len() uint32 {
	return uint32(len(t.metas))
}

func (t *Table) Meta() []*block.Meta {
	return t.metas
}

func (t *Table) SSTID() uint32 {
	return t.id
}
