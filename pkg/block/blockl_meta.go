package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"mini-lsm/pkg/utils"
)

var ErrInvalidBlockMeta = errors.New("invalid block meta")

// Meta is metadata of Block, contains the offset and first key of data
type Meta struct {
	// Offset in data
	Offset uint32
	// FirstKey of this block
	FirstKey []byte
}

// EncodedBlockMeta help append all metaData to bytes buffer
func EncodedBlockMeta(metaList []*Meta) []byte {
	estimateMetadataSize := uint16(0)
	for _, meta := range metaList {
		estimateMetadataSize += SizeOfUint32
		estimateMetadataSize += SizeOfUint16
		estimateMetadataSize += uint16(len(meta.FirstKey))
	}

	var buffer bytes.Buffer
	var buf [SizeOfUint32]byte
	for _, meta := range metaList {
		binary.BigEndian.PutUint32(buf[:SizeOfUint32], meta.Offset)
		buffer.Write(buf[:SizeOfUint32]) // offset in metadata

		binary.BigEndian.PutUint16(buf[:SizeOfUint16], uint16(len(meta.FirstKey)))
		buffer.Write(buf[:SizeOfUint16]) // first key of len
		buffer.Write(meta.FirstKey)      // first key
	}
	utils.Assertf(estimateMetadataSize == uint16(buffer.Len()),
		"buf size error after encoding, estimateMetadataSize: %d should be equal to buffer.Len(): %d", estimateMetadataSize, buffer.Len())

	return buffer.Bytes()
}

// DecodeBlockMeta read []*Meta from byte slice
func DecodeBlockMeta(input []byte) ([]*Meta, error) {
	return DecodeBlockMetaFromReader(bytes.NewReader(input))
}

func readUint32(r io.Reader, buffer []byte) (uint32, error) {
	if buffer == nil {
		buffer = make([]byte, SizeOfUint32)
	}
	n, err := r.Read(buffer[:SizeOfUint32])
	if err != nil {
		if err == io.EOF {
			return 0, io.EOF
		}
		return 0, ErrInvalidBlockMeta
	}
	if uint16(n) != SizeOfUint32 {
		return 0, ErrInvalidBlockMeta
	}
	return binary.BigEndian.Uint32(buffer[:SizeOfUint32]), nil
}

func readUint16(r io.Reader, buffer []byte) (uint16, error) {
	if buffer == nil {
		buffer = make([]byte, SizeOfUint16)
	}
	n, err := r.Read(buffer[:SizeOfUint16])
	if err != nil {
		if err == io.EOF {
			return 0, io.EOF
		}
		return 0, ErrInvalidBlockMeta
	}
	if uint16(n) != SizeOfUint16 {
		return 0, ErrInvalidBlockMeta
	}
	return binary.BigEndian.Uint16(buffer[:SizeOfUint16]), nil
}

// DecodeBlockMetaFromReader reads []*Meta from reader
func DecodeBlockMetaFromReader(r io.Reader) ([]*Meta, error) {
	var metas = make([]*Meta, 0)
	buffer := make([]byte, SizeOfUint32)
	for {
		meta, err := decodeBlock(r, buffer)
		if err == io.EOF {
			return metas, nil
		}
		metas = append(metas, meta)
	}
}

func decodeBlock(r io.Reader, buffer []byte) (*Meta, error) {
	offset, err := readUint32(r, buffer)
	if err != nil {
		return nil, err
	}
	firstKeyLen, err := readUint16(r, buffer)
	if err != nil {
		return nil, err
	}
	key := make([]byte, firstKeyLen)
	n, err := r.Read(key)
	if err != nil {
		return nil, err
	}
	if n != int(firstKeyLen) {
		return nil, ErrInvalidBlockMeta
	}
	return &Meta{Offset: offset, FirstKey: key}, nil
}
