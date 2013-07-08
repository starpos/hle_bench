## Description

Concurrent tree data structure benchmark.

Plan to implement:
* std::map + single mutex.
* std::map + single spinlock.
* std::map + single spinlock + TSX HLE.
* b+tree + single mutex.
* b+tree + single spinlock.
* b+tree + single spinlock + TSX HLE.
* b+tree + multi-granularity lock.
* lock-free b+tree.

## Requirements

* c++11 compiler (gcc-4.8.1 or clang-3.3).

## Lisence

GPLv2 or 3.

