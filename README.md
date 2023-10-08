# mini-lsm
A tutorial about how to build  LSM-Tree storage engine

Build a simple key-value storage engine in a week!

## Tutorial

The tutorial is available at [https://github.com/zhenguozhang/mini-lsm](https://github.com/zhenguozhang/mini-lsm). You can use the provided starter
code to kick off your project, and follow the tutorial to implement the LSM tree.

## Development



If you changed public API in the reference solution, you might also need to synchronize it to the starter crate.

## Progress

The tutorial has 8 parts (which can be finished in 7 days):

* Day 1: Block encoding. SSTs are composed of multiple data blocks. We will implement the block encoding.
* Day 2: SST encoding.
* Day 3: MemTable and Merge Iterators.
* Day 4: Block cache and Engine. To reduce disk I/O and maximize performance, we will use moka-rs to build a block cache
  for the LSM tree. In this day we will get a functional (but not persistent) key-value engine with `get`, `put`, `scan`,
  `delete` API.
* Day 5: Compaction. Now it's time to maintain a leveled structure for SSTs.
* Day 6: Recovery. We will implement WAL and manifest so that the engine can recover after restart.
* Day 7: Bloom filter and key compression. They are widely-used optimizations in LSM tree structures.

We have reference solution up to day 4 and tutorial up to day 4 for now.