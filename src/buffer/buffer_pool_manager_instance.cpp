//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <bits/types/cookie_io_functions_t.h>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/logger.h"
#include "type/integer_type.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/** Helper function 1: Search from free list */
bool BufferPoolManagerInstance::SearchFromFreeList(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    auto i = free_list_.begin();
    *frame_id = *i;
    free_list_.pop_front();  // update free list since we have chosen one from it
    return true;
  }
  return false;
}

/** Helper function 2: Search from LRU replacer */
bool BufferPoolManagerInstance::SearchFromReplacer(frame_id_t *frame_id) {
  if (replacer_->Size() != 0) {
    frame_id_t temp;
    if (!replacer_->Evict(&temp)) {
        return false;
    }
    *frame_id = temp;
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
    std::scoped_lock<std::mutex> lock(this->latch_);
    frame_id_t frame_id;
    bool is_from_replacer = false;
    if (SearchFromFreeList(&frame_id) || (is_from_replacer = SearchFromReplacer(&frame_id))) {
        if (is_from_replacer) {
            // write to disk if dirty and clean the frame
            page_id_t evict_page_id = this->pages_[frame_id].page_id_;
            if (this->pages_[frame_id].IsDirty()) {
                this->disk_manager_->WritePage(evict_page_id, this->pages_[frame_id].GetData());
            }
            this->page_table_->Remove(evict_page_id);
        }
        // update metadata
        *page_id = AllocatePage();
        this->page_table_->Insert(*page_id, frame_id);
        this->pages_[frame_id].ResetMemory();
        this->pages_[frame_id].is_dirty_ = false;
        this->pages_[frame_id].page_id_ = *page_id;
        this->pages_[frame_id].pin_count_++;
        // record access for lru-K and set evictable status
        this->replacer_->RecordAccess(frame_id);
        this->replacer_->SetEvictable(frame_id, false);
        return this->pages_ + frame_id;
    }
    return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
    std::scoped_lock<std::mutex> lock(this->latch_);
    frame_id_t frame_id;
    bool is_fetched = this->page_table_->Find(page_id, frame_id);
    if (is_fetched) {
        this->replacer_->RecordAccess(frame_id);
        this->pages_[frame_id].pin_count_++;
        // have to set evictable status!
        this->replacer_->SetEvictable(frame_id, false);
        return this->pages_ + frame_id;   
    }
    bool is_from_replacer = false;
    if (SearchFromFreeList(&frame_id) || (is_from_replacer = SearchFromReplacer(&frame_id))) {                                                        
        if (is_from_replacer) {                                          
            // write to disk if dirty and clean the frame                
            page_id_t evict_page_id = this->pages_[frame_id].page_id_;   
            if (this->pages_[frame_id].IsDirty()) {                      
                this->disk_manager_->WritePage(evict_page_id, this->pages_[frame_id].GetData());                                                      
            }
            this->page_table_->Remove(evict_page_id);                    
        }   
        // update metadata                                               
        // *page_id = AllocatePage();                                       
        this->page_table_->Insert(page_id, frame_id);                   
        this->pages_[frame_id].ResetMemory();
        this->pages_[frame_id].is_dirty_ = false;                        
        this->pages_[frame_id].page_id_ = page_id;                      
        this->pages_[frame_id].pin_count_ = 1;
        // record access for lru-K and set evictable status              
        this->replacer_->RecordAccess(frame_id);
        this->replacer_->SetEvictable(frame_id, false);
        this->disk_manager_->ReadPage(page_id, this->pages_[frame_id].GetData());
        return this->pages_ + frame_id;
    }   
    return nullptr;

}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
    std::scoped_lock<std::mutex> lock(this->latch_);
    frame_id_t frame_id;
    bool found = this->page_table_->Find(page_id, frame_id);
    if (!found || this->pages_[frame_id].pin_count_ == 0) {
        return false;
    }
    // found in replacer
    this->pages_[frame_id].pin_count_--;
    if (is_dirty) {
        this->pages_[frame_id].is_dirty_ = is_dirty;
    }
    if (this->pages_[frame_id].pin_count_ == 0) {
        this->replacer_->SetEvictable(frame_id, true);
    }
    return true; 
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
    if (page_id == INVALID_PAGE_ID) {
        return false;
    }
    frame_id_t frame_id;
    bool found = this->page_table_->Find(page_id, frame_id);
    if (!found) {
        return false;
    }
    this->disk_manager_->WritePage(page_id, this->pages_[frame_id].GetData());
    this->pages_[frame_id].is_dirty_ = false;
    return true; 
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
    std::scoped_lock<std::mutex> lock(this->latch_);
    for (size_t i = 0; i < this->pool_size_; i++) {
        FlushPgImp(this->pages_[i].page_id_);
    }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
    std::scoped_lock<std::mutex> lock(this->latch_);
    frame_id_t frame_id;
    bool found = this->page_table_->Find(page_id, frame_id); 
    // not found
    if (!found) {
        return true;
    }
    // pinned and cannot be deleted
    if (this->pages_[frame_id].pin_count_ != 0) {
        return false;
    }
    //LOG_INFO("page_table_ = %p, frame_id = %d, page_id = %d", page_table_, frame_id, page_id);
    this->page_table_->Remove(page_id);
    this->replacer_->Remove(frame_id);
    this->free_list_.push_back(frame_id);
    this->pages_[frame_id].is_dirty_ = false;
    this->pages_[frame_id].pin_count_ = 0;
    this->pages_[frame_id].ResetMemory();
    this->pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    DeallocatePage(page_id);
    return true; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}// namespace bustub
