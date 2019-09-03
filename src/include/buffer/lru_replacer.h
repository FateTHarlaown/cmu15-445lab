/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <list>
#include <mutex>
#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

namespace cmudb {

template<typename T>
class LRUReplacer : public Replacer<T> {
public:
    using LockGuard = std::lock_guard<std::mutex>;
    using List = std::list<T>;
    using ElementPtr = typename List::iterator;
    // do not change public interface
    LRUReplacer();

    ~LRUReplacer();

    void Insert(const T &value);

    bool Victim(T &value);

    bool Erase(const T &value);

    size_t Size();

private:
    // add your member variables here
    uint64_t size_;
    std::mutex mu_;
    std::list<T> list_;
    ExtendibleHash<T, ElementPtr> hash_;
};

} // namespace cmudb
