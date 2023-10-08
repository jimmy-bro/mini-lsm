package block

import (
	"encoding/binary"

	"mini-lsm/pkg/utils"
)

// Builder is used to build a block
// implement an Add func for appending key value on Builder's data
// Builder build Block in this layout following:
// data:
// keyLen | key | valueLen | value
type Builder struct {
	offsets []uint16

	data       []byte
	dataCursor int

	blockSize uint16
}

// NewBlockBuilder return a Builder for giving size
func NewBlockBuilder(size uint16) *Builder {
	return &Builder{
		offsets:    make([]uint16, 0),
		data:       utils.GlobalPool.Get(int(size)),
		dataCursor: 0,
		blockSize:  size,
	}
}

// currentSize is for estimateSize for Block
// layout of Block is like this:
// | offsetLen | offset0(2 Byte) | offset1 ... | offsetN | dataLen | data(N Byte)  |
// Builder can estimate size of a Block
func (b *Builder) currentSize() uint16 {
	return uint16(b.dataCursor)
}

func (b *Builder) IsEmpty() bool {
	return len(b.offsets) == 0
}

type stringOrByteSlice interface {
	string | []byte
}

func estimateGrow[T stringOrByteSlice](key, value T) uint16 {
	return uint16(len(key)) + uint16(len(value)) +
		SizeOfUint16*2 + SizeOfUint16
}

// Add receives a pair of key value(string), return whether it was added to builder
func (b *Builder) Add(key, value string) bool {
	utils.Assert(key != "", "expect none empty key")

	if b.currentSize()+estimateGrow(key, value) > b.blockSize &&
		!b.IsEmpty() {
		return false
	}
	b.offsets = append(b.offsets, b.currentSize())

	binary.BigEndian.PutUint16(b.data[b.dataCursor:b.dataCursor+int(SizeOfUint16)], uint16(len(key)))
	b.dataCursor += int(SizeOfUint16)

	b.dataCursor += copy(b.data[b.dataCursor:], key)

	binary.BigEndian.PutUint16(b.data[b.dataCursor:b.dataCursor+int(SizeOfUint16)], uint16(len(value)))
	b.dataCursor += int(SizeOfUint16)

	b.dataCursor += copy(b.data[b.dataCursor:], value)
	return true
}

// AddByte receives a pair of key value([]byte), return whether it was added to builder
func (b *Builder) AddByte(key, value []byte) bool {
	utils.Assert(len(key) != 0, "expect none empty key")

	if b.currentSize()+estimateGrow(key, value) > b.blockSize &&
		!b.IsEmpty() {
		return false
	}
	b.offsets = append(b.offsets, uint16(b.dataCursor))

	binary.BigEndian.PutUint16(b.data[b.dataCursor:b.dataCursor+int(SizeOfUint16)], uint16(len(key)))
	b.dataCursor += int(SizeOfUint16)

	b.dataCursor += copy(b.data[b.dataCursor:], key)

	binary.BigEndian.PutUint16(b.data[b.dataCursor:b.dataCursor+int(SizeOfUint16)], uint16(len(value)))
	b.dataCursor += int(SizeOfUint16)

	b.dataCursor += copy(b.data[b.dataCursor:], value)
	return true
}

// Build return the Block which Builder built
func (b *Builder) Build() *Block {
	utils.Assert(!b.IsEmpty(),
		"expect builder is not empty")

	return &Block{
		data:    b.data[:b.dataCursor],
		offsets: b.offsets,
	}
}
