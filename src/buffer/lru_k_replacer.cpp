//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <mutex>
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::scoped_lock<std::mutex> lock(this->latch_);    
    // first search in history fifo
    for (auto frame_node_ptr : this->history_list_) {
        if (frame_node_ptr->evictable) {
            this->history_list_.remove(frame_node_ptr);
            this->frame_to_history_map_.erase(frame_node_ptr->frame_id_);
            this->curr_size_ --;
            *frame_id = frame_node_ptr->frame_id_;
            return true;
        }
    }
    // otherwise search in lruk cache
    for (auto frame_node_ptr : this->lruk_list_) {
        if (frame_node_ptr->evictable) {
            this->lruk_list_.remove(frame_node_ptr);
            this->frame_to_lruk_map_.erase(frame_node_ptr->frame_id_);
            this->curr_size_ --;
            *frame_id = frame_node_ptr->frame_id_;
            return true;
        }
    }
    return false; 
}


void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    // TODO: add lock
    std::scoped_lock<std::mutex> lock(this->latch_);
    BUSTUB_ASSERT((int)frame_id <= (int)this->replacer_size_, "Invalid parameter of RecordAccess: frame_id > replacer_size_");
    // check lruk replacer and history fifo
    if (this->frame_to_lruk_map_.find(frame_id) == this->frame_to_lruk_map_.end()) {
        if (this->frame_to_history_map_.find(frame_id) == this->frame_to_history_map_.end()) {
            // case 1: not in both, note that it's impossible that 
            assert(this->curr_size_ < this->replacer_size_);
            // create a new node and insert to history
            auto frame_node_ptr = std::make_shared<FrameNode>(frame_id);
            this->history_list_.push_back(frame_node_ptr);
            this->frame_to_history_map_[frame_id] = frame_node_ptr;
            // this->curr_size_ ++;
        } else {    
            // case 2: found in history
            auto frame_node_ptr = this->frame_to_history_map_[frame_id];
            frame_node_ptr->counter++;
            // move to lruk if counter == k
            if (frame_node_ptr->counter == (int)this->k_) {
                this->history_list_.remove(frame_node_ptr);
                this->frame_to_history_map_.erase(frame_id);
                this->lruk_list_.push_back(frame_node_ptr);
                this->frame_to_lruk_map_[frame_id] = frame_node_ptr;
            }
        }
    } else {    
        // case 3: found in lruk replacer
        auto frame_node_ptr = this->frame_to_lruk_map_[frame_id];
        frame_node_ptr->counter++;
        this->lruk_list_.remove(frame_node_ptr);
        this->lruk_list_.push_back(frame_node_ptr);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(this->latch_);
    if (this->frame_to_lruk_map_.find(frame_id) != this->frame_to_lruk_map_.end()) {
        if (this->frame_to_lruk_map_[frame_id]->evictable != set_evictable) {
            if (set_evictable) {
                this->curr_size_ ++; 
            }
            else { 
                this->curr_size_ --; 
            }
            this->frame_to_lruk_map_[frame_id]->evictable = set_evictable;
        }
        return;
    }

    if (this->frame_to_history_map_.find(frame_id) != this->frame_to_history_map_.end()) {
        if (this->frame_to_history_map_[frame_id]->evictable != set_evictable) {
            if (set_evictable) { 
                this->curr_size_ ++; 
            }
            else { 
                this->curr_size_ --; 
            }
            this->frame_to_history_map_[frame_id]->evictable = set_evictable;
        }
        return;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(this->latch_);
    if (this->frame_to_lruk_map_.find(frame_id) != this->frame_to_lruk_map_.end()) {
        auto frame_node_ptr = this->frame_to_lruk_map_[frame_id];
        BUSTUB_ASSERT(frame_node_ptr->evictable, "Remove() func called on unevitable frame");
        this->frame_to_lruk_map_.erase(frame_id);
        this->lruk_list_.remove(frame_node_ptr);
        this->curr_size_ --;
        return;
    }
   
    if (this->frame_to_history_map_.find(frame_id) != this->frame_to_history_map_.end()) {
        auto frame_node_ptr = this->frame_to_history_map_[frame_id];
        BUSTUB_ASSERT(frame_node_ptr->evictable, "Remove() func called on unevitable frame");
        this->frame_to_history_map_.erase(frame_id);
        this->history_list_.remove(frame_node_ptr);
        this->curr_size_ --;
        return;
    }
    
}

auto LRUKReplacer::Size() -> size_t { 
    std::scoped_lock<std::mutex> lock(this->latch_);
    return this->curr_size_; 
}

}  // namespace bustub
