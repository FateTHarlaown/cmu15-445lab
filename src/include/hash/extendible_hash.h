/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <mutex>

#include "hash/hash_table.h"

namespace cmudb {

template<typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
    struct Bucket {
        explicit Bucket(uint64_t depth) : localDepth(depth), flag(0) {
            mask = (1u << localDepth) - 1;
        }
        std::map<K, V> store;
        uint64_t localDepth;
        uint64_t mask;
        uint64_t flag;
    };

    using BucketPtr = std::shared_ptr<Bucket>;
    using LockGuard = std::lock_guard<std::mutex>;

    // constructor
    explicit ExtendibleHash(size_t size);
    // helper function to generate hash addressing
    size_t HashKey(const K &key);
    // helper function to get global & local depth
    int GetGlobalDepth() const;
    int GetLocalDepth(int bucket_id) const;
    int GetNumBuckets() const;
    // lookup and modifier
    bool Find(const K &key, V &value) override;
    bool Remove(const K &key) override;
    void Insert(const K &key, const V &value) override;

private:
    void extend(uint64_t id);
    void incDepth();

private:
    // add your own member va
    const uint64_t MaxNumKeyPerBucket;

    mutable std::mutex mu_;
    std::vector<BucketPtr> buckets_;
    uint64_t globalDepth_;
    uint64_t globalMask_;
};
} // namespace cmudb
