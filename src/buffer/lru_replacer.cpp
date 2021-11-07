//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (map_.empty()) {
    return false;
  }
  frame_id_t to_delete = list_.back();
  list_.pop_back();
  map_.erase(map_.find(to_delete));
  *frame_id = to_delete;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto find = map_.find(frame_id);
  if (find == map_.end()) {
    return;
  }
  list_.erase(find->second);
  map_.erase(find);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto find = map_.find(frame_id);
  if (find != map_.end()) {
    return;
  }
  list_.emplace_front(frame_id);
  map_[frame_id] = list_.begin();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock_guard(latch_);
  return list_.size();
}

}  // namespace bustub
