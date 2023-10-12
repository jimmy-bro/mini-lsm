# 3. **Mem Table and Merge Iterators**

In this part, you will need to modify:

- `pkg/iterators/merge_iterator.go`
- `pkg/iterators/two_merge_iterator.go`
- `pkg/mem_table/mem_table.go`

This is the last part for the basic building blocks of an LSM tree. After implementing the merge iterators, we can easily merge data from different part of the data structure (mem table + SST) and get an iterator over all data. And in part 4, we will compose all these things together to make a real storage engine.

## Task 1 - Mem Table

In this tutorial, we use [github.com/huandu/skiplist](http://github.com/huandu/skiplist) as the implementation of memtable. Skiplist is like linked-list, where data is stored in a list node and will not be moved in memory. Instead of using a single pointer for the next element, the nodes in skiplists contain multiple pointers and allow user to "skip some elements", so that we can achieve `O(log n)` search, insertion, and deletion.

In storage engine, users will create iterators over the data structure. Generally, once user modifies the data structure, the iterator will become invalid (which is the case for C++ STL and Rust containers). However, skiplists allow us to access and modify the data structure at the same time, therefore potentially improving the performance when there is concurrent access. There are some papers argue that skiplists are bad, but the good property that data stays in its place in memory can make the implementation easier for us.

In `mem_table.go`, you will need to implement a mem-table based on crossbeam-skiplist. Note that the memtable only supports `get`, `scan`, and `put` without `delete`. The deletion is represented as a tombstone `key -> empty value`, and the actual data will be deleted during the compaction process (day 5). Note that all `get`, `scan`, `put` functions only need `&self`, which means that we can concurrently call these operations.

## Task 2 - Mem Table Iterator

You can now implement an iterator `MemTableIterator` for `MemTable`. `func (t *Table) Scan(lower, upper []byte) *Iterator` will create an iterator that returns all elements within the range `lower, upper`.

Note that `huandu-skiplist`'s iterator has the same lifetime as the skiplist itself, which means that we will always need to provide a lifetime when using the iterator.

```go

type Iterator struct {
	ele *skiplist.Element
	end []byte
}

```

In this design, you might have noticed that as long as we have the iterator object, the mem-table cannot be freed from the memory. In this tutorial, we assume user operations are short, so that this will not cause big problems. See extra task for possible improvements.

## Task 3 - Merge Iterator[havn’t finished]

## Task 4 - Two Merge Iterator

The LSM has two structures for storing data: the mem-tables in memory, and the SSTs on disk. After we constructed the iterator for all SSTs and all mem-tables respectively, we will need a new iterator to merge iterators of two different types. That is `TwoMergeIterator`.

You can implement `TwoMergeIterator` in `two_merge_iter.go`. Similar to `MergeIterator`, if the same key is found in both of the iterator, the first iterator takes precedence.

## Extra Tasks

- Implement different mem-table and see how it differs from skiplist. i.e., BTree mem-table. You will notice that it is hard to get an iterator over the B+ tree without holding a lock of the same timespan as the iterator. You might need to think of smart ways of solving this.
- Async iterator. One interesting thing to explore is to see if it is possible to asynchronize everything in the storage engine. You might find some lifetime related problems and need to workaround them.
- Foreground iterator. In this tutorial we assumed that all operations are short, so that we can hold reference to mem-table in the iterator. If an iterator is held by users for a long time, the whole mem-table (which might be 256MB) will stay in the memory even if it has been flushed to disk. To solve this, we can provide a `ForegroundIterator` / `LongIterator` to our user. The iterator will periodically create new underlying storage iterator so as to allow garbage collection of the resources.