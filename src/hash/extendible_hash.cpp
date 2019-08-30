#include <list>
#include <algorithm>
#include <iostream>
#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : MaxNumKeyPerBucket(size), globalDepth_(0) {
    BucketPtr p = std::make_shared<Bucket>(0);
    buckets_.push_back(p);
    globalMask_ = 0;
}

/*
 * helper function to calculate the hashing address of input key
 */
template<typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    return (std::hash<K>()(key));
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template<typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    LockGuard guard(mu_);
    return globalDepth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template<typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    LockGuard guard(mu_);
    return buckets_[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template<typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    LockGuard guard(mu_);
    return buckets_.size();
}

/*
 * lookup function to find value associate with input key
 */
template<typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    LockGuard guard(mu_);
    size_t bucket_id = HashKey(key) & globalMask_;
    BucketPtr & bucket = buckets_[bucket_id];
    auto it = bucket->store.find(key);
    if (it != bucket->store.end()) {
        value = it->second;
        return true;
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template<typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
    LockGuard guard(mu_);
    size_t id = HashKey(key) & globalMask_;
    BucketPtr & bucket = buckets_[id];
    return bucket->store.erase(key) > 0;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template<typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    LockGuard guard(mu_);
    size_t id = HashKey(key) & globalMask_;
    BucketPtr bucket = buckets_[id];
    uint64_t n = bucket->store.size();
    while ( n >= MaxNumKeyPerBucket) {
        extend(id);
        id = HashKey(key) & globalMask_;
        bucket = buckets_[id];
        n = bucket->store.size();
    }
    bucket->store.insert(std::make_pair(key, value));
}

/*
 * extend hash table, param `id` means which bucket caused.
 */
template<typename K, typename V>
void ExtendibleHash<K, V>::extend(uint64_t id) {
    BucketPtr bucket = buckets_[id];
    bucket->localDepth += 1;
    bucket->mask = (1u << bucket->localDepth) - 1;

    BucketPtr newBucket = std::make_shared<Bucket>(bucket->localDepth);
    newBucket->mask = bucket->mask;
    newBucket->flag = bucket->flag + (1u << (bucket->localDepth-1));

    auto inserter = [&](const auto & pair) {
        uint64_t flag = HashKey(pair.first) & newBucket->mask;
        if (flag == newBucket->flag) {
            newBucket->store.insert(pair);
        }
    };
    std::for_each(bucket->store.begin(), bucket->store.end(), inserter);
    for (const auto & pair : newBucket->store) {
        bucket->store.erase(pair.first);
    }

    if (bucket->localDepth > globalDepth_) {
        const uint64_t num = buckets_.size();
        for (uint64_t i = num; i < 2 * num; ++i) {
            uint64_t previousId = i & globalMask_;
            BucketPtr p = buckets_[previousId];
            buckets_.push_back(p);
        }
        globalDepth_++;
        globalMask_ = (1u << globalDepth_) - 1;
    }

    for (uint64_t i = 0; i < buckets_.size(); ++i) {
        if ((i & newBucket->mask) == newBucket->flag) {
            buckets_[i] = newBucket;
        }
    }
}

template<typename K, typename V>
void ExtendibleHash<K, V>::incDepth() {

}
template
class ExtendibleHash<page_id_t, Page *>;

template
class ExtendibleHash<Page *, std::list<Page *>::iterator>;

// test purpose
template
class ExtendibleHash<int, std::string>;

template
class ExtendibleHash<int, std::list<int>::iterator>;

template
class ExtendibleHash<int, int>;
} // namespace cmudb
