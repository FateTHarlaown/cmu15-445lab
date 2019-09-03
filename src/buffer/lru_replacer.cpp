/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template<typename T>
LRUReplacer<T>::LRUReplacer() : size_(0), hash_(2) {

}

template<typename T>
LRUReplacer<T>::~LRUReplacer() {

}

/*
 * Insert value into LRU
 */
template<typename T>
void LRUReplacer<T>::Insert(const T &value) {
    LockGuard guard(mu_);
    ElementPtr p;
    if (hash_.Find(value, p)) {
        list_.erase(p);
    } else {
        ++size_;
    }
    list_.push_front(value);
    hash_.Insert(value, list_.begin());
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template<typename T>
bool LRUReplacer<T>::Victim(T &value) {
    LockGuard guard(mu_);
    if (list_.empty()) {
        return false;
    }
    auto p = (--list_.end());
    value = *p;
    hash_.Remove(*p);
    list_.pop_back();
    --size_;
    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template<typename T>
bool LRUReplacer<T>::Erase(const T &value) {
    LockGuard guard(mu_);
    ElementPtr p;
    if (!hash_.Find(value, p)) {
        return false;
    }

    hash_.Remove(value);
    list_.erase(p);
    --size_;

    return true;
}

template<typename T>
size_t LRUReplacer<T>::Size() {
    LockGuard guard(mu_);
    return size_;
}

template
class LRUReplacer<Page *>;

// test only
template
class LRUReplacer<int>;

} // namespace cmudb
