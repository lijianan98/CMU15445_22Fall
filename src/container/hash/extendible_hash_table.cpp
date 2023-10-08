//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include <cmath>

#include "container/hash/extendible_hash_table.h"
#include "primer/p0_trie.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
    dir_.push_back(std::make_shared<Bucket>(bucket_size));    
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
    // TODO: lock (read_lock)
    std::scoped_lock<std::mutex> lock(latch_);
    size_t dir_index = IndexOf(key);
    // std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table
    std::shared_ptr<Bucket> bucket_ptr = dir_[dir_index];
    return bucket_ptr->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
    // TODO: lock, write lock, and we don't do shrinking
    std::scoped_lock<std::mutex> lock(latch_);
    size_t dir_index = IndexOf(key);
    std::shared_ptr<Bucket> bucket_ptr = dir_[dir_index];
    return bucket_ptr->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) {
    // when this function is called, global_depth should have been updated if dir grows
    // int local_depth = bucket->GetDepth();
    // int local_high_bit = 1 << local_depth;
    for (auto &item : bucket->GetItems()) {
        size_t idx = IndexOf(item.first);
        dir_[idx]->Insert(item.first, item.second);
    }
}


template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
    // TODO: lock, write lock
    std::scoped_lock<std::mutex> lock(latch_);
    size_t dir_index = IndexOf(key);
    std::shared_ptr<Bucket> bucket_ptr = dir_[dir_index];
    // loop until insertion succeeds
    while (bucket_ptr->Insert(key, value) == false) {
        int local_depth = bucket_ptr->GetDepth();
        if (local_depth == global_depth_) {  // double directory size
            int old_dir_size = 1 << global_depth_;
            global_depth_++;
            for (int i = 0; i < old_dir_size; i++) {
                dir_.push_back(dir_[i]);   // update increase part of dir_
            }
        }
        std::shared_ptr<Bucket> bucket1_ptr = std::make_shared<Bucket>(this->bucket_size_, local_depth + 1);
        std::shared_ptr<Bucket> bucket2_ptr = std::make_shared<Bucket>(this->bucket_size_, local_depth + 1);
        // example: g:3,l:2 => range:2 lhb:4 si:dir_idx&0x11
        int range = (int)std::pow(2, global_depth_ - local_depth);
        int local_high_bit = 1 << local_depth;
        int interval = local_high_bit; 
        int start_idx = dir_index & (local_high_bit - 1);
        // if idx at (1 << local_depth) bit == 0 => bucket1, else bucket2
        // example: 0(11) => bucket1, 1(11) => bucket2
        for (int i = 0; i < range; i++) {
            if (start_idx & local_high_bit) {
                dir_[start_idx] = bucket1_ptr;
            } else {
                dir_[start_idx] = bucket2_ptr;
            }
            start_idx += interval;
        }

        // re-distribute original bucket to bucket1 and bucket2
        RedistributeBucket(bucket_ptr);    
        num_buckets_++; // update bucket number    

        // update position of where the new KV pair should be inserted
        dir_index = IndexOf(key);
        bucket_ptr = dir_[dir_index];
    }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
    for (auto &item : list_) {
        if (item.first == key) {
            value = item.second;
            return true;
        }
    }
    return false;        
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
    for (auto item = list_.begin(); item != list_.end(); item++) {
        if (item->first == key) {
            list_.erase(item);
            return true;
        }
    }
    return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
    for (auto item = list_.begin(); item != list_.end(); item++) {
        if (item->first == key) {
            item->second = value;
            return true;
        }
    }
    if (IsFull()) {
        return false;
    } else {
        // list_.push_front(std::make_pair(key, value));
        list_.emplace_front(key, value);
        return true;
    }
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
