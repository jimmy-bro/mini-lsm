package block

import (
	"bytes"
	"encoding/binary"

	"mini-lsm/pkg/utils"
)

// Iter can hold an Block, for iterating it one-by-one.
// keys in Iter should be sorted
type Iter struct {
	block *Block
	key   []byte
	value []byte
	idx   uint64
}

// NewBlockIter receives a block and return Iter for it.
func NewBlockIter(block *Block) *Iter {
	return &Iter{
		block: block,
		key:   make([]byte, 0),
		value: make([]byte, 0),
		idx:   0,
	}
}

// NewBlockIterAndSeekToFirst receives a block, create a Iter, seek to first key, return it.
func NewBlockIterAndSeekToFirst(block *Block) *Iter {
	iter := NewBlockIter(block)
	iter.SeekTo(0)
	return iter
}

// NewBlockIterAndSeekToKey receives a block, create a Iter, seek to specified key, return it.
func NewBlockIterAndSeekToKey(block *Block, key []byte) *Iter {
	i := NewBlockIter(block)
	i.SeekToKey(key)
	return i
}

// IsValid checks that whether Iter valid
func (b *Iter) IsValid() bool {
	return b != nil && b.block != nil && len(b.key) != 0
}

// SeekToFirst help Iter to seek to first key
func (b *Iter) SeekToFirst() {
	b.SeekTo(0)
}

// Key get key for current pos
func (b *Iter) Key() []byte {
	utils.Assert(len(b.key) != 0, "invalid iterator, you should call IsValid to check iter valid")
	// WARNING: we assumed that return key will not be modified
	// key := make([]byte, len(b.key))
	// copy(key, b.key)
	// return key
	return b.key
}

// Value get value for current pos
func (b *Iter) Value() []byte {
	utils.Assert(len(b.key) != 0, "invalid iterator, you should call IsValid to check iter valid")
	// WARNING: we assumed that return value will not be modified
	// value := make([]byte, len(b.value))
	// copy(value, b.value)
	// return value
	return b.value
}

// SeekTo receives an index, then try to seek to key-value pair on this index.
func (b *Iter) SeekTo(idx uint64) {
	if b.block == nil {
		return
	}
	if idx >= uint64(len(b.block.offsets)) {
		b.key = nil
		b.value = nil
		return
	}
	offset := uint64(b.block.offsets[idx])
	b.seekToOffset(offset)
	b.idx = idx
}

// Next make iter turn to next key-value pair
func (b *Iter) Next() {
	if b.block == nil {
		return
	}
	b.idx++
	b.SeekTo(b.idx)
}

// SeekToKey make iter to find key in dichotomy.
func (b *Iter) SeekToKey(key []byte) {
	if b.block == nil {
		return
	}
	low := 0
	high := len(b.block.offsets)

	for low < high {
		mid := low + (high-low)/2
		b.SeekTo(uint64(mid))

		utils.Assert(b.IsValid(), "encountered invalid block")

		switch bytes.Compare(b.key, key) {
		case 0:
			return
		case -1:
			low = mid + 1
		case 1:
			high = mid
		}
	}

	b.SeekTo(uint64(low))
}

func (b *Iter) seekToOffset(offset uint64) {
	utils.Assertf(offset < uint64(len(b.block.data)),
		"offset should be less than block data, offset: %d, len(b.block.data): %d", offset, len(b.block.data))
	entry := b.block.data[offset:]

	keyLen := binary.BigEndian.Uint16(entry[:2])
	entry = entry[2:]
	b.key = append(b.key[:0], entry[:keyLen]...)
	entry = entry[keyLen:]

	valueLen := binary.BigEndian.Uint16(entry[:2])
	entry = entry[2:]
	b.value = append(b.value[:0], entry[:valueLen]...)
}
