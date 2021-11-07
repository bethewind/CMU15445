//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto find = page_table_.find(page_id);
  // if find pages, increase pin_count and return.
  if (find != page_table_.end()) {
    Page *ret = pages_ + find->second;
    ret->pin_count_ += 1;
    replacer_->Pin(find->second);
    return ret;
  }
  frame_id_t loc{-1};
  if (!free_list_.empty()) {
    // first search free list.
    loc = free_list_.back();
    free_list_.pop_back();
  } else {
    // then search the replacer.
    bool success = replacer_->Victim(&loc);
    if (!success) {
      return nullptr;
    }
  }
  // if the data is dirty, write back.
  if (pages_[loc].is_dirty_) {
    disk_manager_->WritePage(pages_[loc].page_id_, pages_[loc].data_);
    page_table_.erase(page_table_.find(pages_[loc].page_id_));
  }
  // read data from disk and update metadata.
  disk_manager_->ReadPage(page_id, pages_[loc].data_);
  // update page_table.
  page_table_[page_id] = loc;
  pages_[loc].page_id_ = page_id;
  pages_[loc].is_dirty_ = false;
  pages_[loc].pin_count_ = 1;
  return pages_ + loc;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  // if not find the page, return false.
  auto find = page_table_.find(page_id);
  if (find == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = find->second;
  // if pin_count_ <= 0, return false;
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_ -= 1;
  pages_[frame_id].is_dirty_ = pages_[frame_id].is_dirty_ || is_dirty;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto find = page_table_.find(page_id);
  if (find == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = find->second;
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  }
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t loc{-1};
  if (!free_list_.empty()) {
    loc = free_list_.back();
    free_list_.pop_back();
  } else {
    bool success = replacer_->Victim(&loc);
    if (!success) {
      return nullptr;
    }
  }

  // if the data is dirty, write back.
  if (pages_[loc].is_dirty_) {
    disk_manager_->WritePage(pages_[loc].page_id_, pages_[loc].data_);
  }

  if (pages_[loc].page_id_ != INVALID_PAGE_ID) {
    page_table_.erase(page_table_.find(pages_[loc].page_id_));
  }
  pages_[loc].ResetMemory();
  *page_id = disk_manager_->AllocatePage();
  pages_[loc].page_id_ = *page_id;
  pages_[loc].pin_count_ = 1;
  pages_[loc].is_dirty_ = false;
  replacer_->Pin(*page_id);
  page_table_[*page_id] = loc;
  return pages_ + loc;
  // return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto find = page_table_.find(page_id);
  if (find == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = find->second;
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }

  page_table_.erase(find);
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();
  replacer_->Unpin(page_id);
  free_list_.emplace_back(frame_id);
  disk_manager_->DeallocatePage(page_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::mutex> lock_guard(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].is_dirty_) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
    }
  }
}

}  // namespace bustub
