package sst

import (
	"bufio"
	"encoding/binary"
	"math"
	"os"
	"sync"

	"mini-lsm/pkg/block"
	"mini-lsm/pkg/utils"
)

// TableBuilder can build sst
// 3. save meta for block to metas
type TableBuilder struct {
	// builder is current Block Builder
	builder *block.Builder

	// firstKey: save firstKey for every Block
	firstKey []byte

	// data: append encoded Block to
	data     [][]byte
	dataSize int64

	// metas saves every meta for built Block
	metas []*block.Meta

	// blockSize is size of every Block
	blockSize uint16
}

func deepcopy(key []byte) []byte {
	out := make([]byte, len(key))
	copy(out, key)
	return out
}

// NewTableBuilder receives max blockSize and return a TableBuilder
func NewTableBuilder(blockSize uint16) *TableBuilder {
	return &TableBuilder{
		builder:   block.NewBlockBuilder(blockSize),
		metas:     make([]*block.Meta, 0),
		blockSize: blockSize,
	}
}

// Add receives a pair of key value(string), if builder has been full, we'll close
// current block, create new Block then add key-value to it.
func (t *TableBuilder) Add(key, value string) {
	if t.firstKey == nil {
		t.firstKey = []byte(key)
	}
	if t.builder.Add(key, value) {
		return
	}
	t.finishBlock()
	if !t.builder.Add(key, value) {
		panic("build error")
	}
	t.firstKey = []byte(key)
}

// AddByte receives a pair of key value([]byte), if builder has been full, we'll close
// current block, create new Block then add key-value to it.
func (t *TableBuilder) AddByte(key, value []byte) {
	if t.firstKey == nil {
		t.firstKey = deepcopy(key)
	}
	if t.builder.AddByte(key, value) {
		return
	}
	t.finishBlock()
	utils.Assert(t.builder.AddByte(key, value), "table builder add key value failed")
	t.firstKey = deepcopy(key)
}

// Build build sst with all built block
// WARNING: after Build calling
// the data in TableBuilder is dirty(other metadata was appended to it)
func (t *TableBuilder) Build(id uint32, cache *sync.Map, path string) (*Table, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	t.finishBlock()
	blockMeta := block.EncodedBlockMeta(t.metas)
	bw := bufio.NewWriter(fd)
	written := 0
	for i := range t.data {
		n, err := bw.Write(t.data[i])
		if err != nil {
			return nil, err
		}
		written += n
		utils.GlobalPool.Put(t.data[i])
	}
	utils.Assertf(written == int(t.dataSize), "mismatch data size write to sst file, written(%d) != t.dataSize(%d)", written, t.dataSize)
	n, err := bw.Write(blockMeta)
	if err != nil {
		return nil, err
	}
	utils.Assertf(n == len(blockMeta), "mismatch block meta size write to sst file")

	metaOffset := t.dataSize
	utils.Assertf(metaOffset < math.MaxUint32, "metaOffset %d should be less than 1<<32-1", metaOffset)
	var buf [block.SizeOfUint32]byte
	binary.BigEndian.PutUint32(buf[:], uint32(metaOffset))
	_, err = bw.Write(buf[:])
	if err != nil {
		return nil, err
	}
	err = bw.Flush()
	if err != nil {
		return nil, err
	}
	err = fd.Sync()
	if err != nil {
		return nil, err
	}
	return &Table{
		id:          id,
		fd:          fd,
		metas:       t.metas,
		metaOffsets: uint32(metaOffset),
		blockCache:  cache,
	}, nil
}

func (t *TableBuilder) Len() uint32 {
	return uint32(len(t.metas))
}

func (t *TableBuilder) finishBlock() {
	builder := t.builder
	if !builder.IsEmpty() {
		t.metas = append(t.metas, &block.Meta{
			Offset:   uint32(t.dataSize),
			FirstKey: deepcopy(t.firstKey),
		})
		data := builder.Build().Encode()
		t.data = append(t.data, data)
		t.dataSize += int64(len(data))
	}
	t.builder = block.NewBlockBuilder(t.blockSize)
}
