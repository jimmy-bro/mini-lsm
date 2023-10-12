# 1. Store key-value pairs in little blocks

In this part, you will need to modify:

- `pkg/block/block.go`
- `pkg/block/block_builder.go`
- `pkg/block/block_iter.go`
- `pkg/block/block_meta.go`

After you have finished this part, you can use `pkg/block/block_test.go` to run and pass all test cases .If you want to add your own test cases, feel free to write your test cases in this test cases file.

## Task 1 - Block Builder

Block is the minimum read unit in LSM. It is of 4KB size in general, similar to database pages. In each block, we will store a sequence of sorted key-value pairs.

You will need to modify `Builder` in `pkg/block/block_builder.go` to build the encoded data and the offset array.The block contains two parts: data and offsets.

  

```go
---------------------------------------------------------------------
|          data         |          offsets          |      meta     |
|-----------------------|---------------------------|---------------|
|entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|
---------------------------------------------------------------------
```

When user adds a key-value pair to a block (which is an entry), we will need to serialize it into the following format:

```go
-----------------------------------------------------------------------
|                           Entry #1                            | ... |
-----------------------------------------------------------------------
| key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
-----------------------------------------------------------------------
```

Key length and value length are both 2 bytes, which means their maximum lengths are 65535.(Internally stored as `uint16`)

We assume that keys will never be empty, and values can be empty. An empty value means that the corresponding key has been deleted in the view of other parts of the system.  For the `Builder` and `Iterator`, we just treat the empty value as-is.

At the end of each block, we will store the offsets of each each entry and the total number of entries. For example, if the first entry is at 0th position of the block, and the second entry is at 12th position of the block.

```go
-------------------------------
|offset|offset|num_of_elements|
-------------------------------
|   0  |  12  |       2       |
-------------------------------
```

The footer of the block will be as above. Each of the number is stored as `uint16.`

The block has a size limit, which is `target_size` .Unless the first key-value pair exceeds the target block size, you should ensure that the encoded block size is always less than or equal to `target_size`. (In the provided code, the  `target_size` here is essentially the `block_size`)

The Builder will produce the data part and unencoded entry offsets when build is called. The infomation will be stored in the `Block` struct. As key-value entries are stored in raw format and offsets are stored in a separate vector, this reduces unnecessary memoryallocations and processing overhead when decoding data —what you need to do is to simply copy the raw block data to the `data` vector and decode the entry offsets every 2 bytes, *instead of* creating something like **`[]struct{ First []byte; Second []byte }**` to store all the key-value pairs in one block in memory. This compact memory layout is very efficient.

For the encoding and decoding part, you'll need to modify `Block` in `pkg/block/block.go`. Specifically, you are required to implement `func Encode` and `func Decode`, which will encode to / decode from the data layout illustrated in the above figures.

## Task 2 - Block Iterator

Given a `Block` object, we will need to extract the key-value pairs. To do this, we create an iterator over a block and find the information we want.

`BlockIterator` can be created with an `*Block`. If `NewBlockIterAndSeekToFirst` is called, it will be positioned at the first key in the block. If `NewBlockIterAndSeekToKey` is called, the iterator will be positioned at the first key that is `>=` the provided key.  For example, if `1, 3, 5` is in a block.

```go
iter := NewBlockIterAndSeekToKey(block, []byte("2"))
assert.Equal(t, []byte("2"), iter.Key())
```

The above `seek 2` will make the iterator to be positioned at the next available key of `2`, which in this case is `3`.

The iterator should copy `key` and `value` from the block and store them inside the iterator, so that users can access the key and the value without any extra copy with `func (b *Iter) Key() []byte`, which directly returns the reference of the locally-stored key and value.

When `next` is called, the iterator will move to the next position. If we reach the end of the block, we can set `key` to empty and return `false` from `isValid`, so that the caller can switch to another block if possible.

After implementing this part, you should be able to pass all tests in `pkg/block/block_test.go`.

## Extra Tasks

Here is a list of extra tasks you can do to make the block encoding more robust and efficient.

*Note: Some test cases might not pass after implementing this part. You might need to write your own test cases.*

- Implement block checksum. Verify checksum when decoding the block.
- Compress / Decompress block. Compress on `build` and decompress on decoding.
