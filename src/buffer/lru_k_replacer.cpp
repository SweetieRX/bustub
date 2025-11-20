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
#include <exception>
#include <mutex>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto it = history_list_.rbegin(); it != history_list_.rend(); ++it) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      access_count_[frame] = 0;
      is_evictable_[frame] = false;
      history_list_.erase(history_map_[frame]);
      history_map_.erase(frame);
      *frame_id = frame;
      curr_size_--;
      return true;
    }
  }
  for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); ++it) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      access_count_[frame] = 0;
      is_evictable_[frame] = false;
      cache_list_.erase(cache_map_[frame]);
      cache_map_.erase(frame);
      *frame_id = frame;
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  access_count_[frame_id]++;
  if (access_count_[frame_id] == k_) {
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
    history_list_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);
  } else if (access_count_[frame_id] > k_) {
    if (cache_map_.count(frame_id)) {
      cache_list_.erase(cache_map_[frame_id]);
    }
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else {
    if (!history_map_.count(frame_id)) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  /*
  if (access_count_[frame_id] == 0) {
    return;
  }
  这一步避免了， is_evictable_[frame_id]当frame_id不存在创建is_evictable_[frame_id]=false的情况;
  并且，在调用Rmove时会产生access_count_[frame_id] = 0，页面已经被删除。
*/
  if (access_count_[frame_id] == 0) {
    return;
  }

  if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  }
  if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  auto cnt = access_count_[frame_id];
  if (cnt == 0) {
    return;
  }
  if (!is_evictable_[frame_id]) {
    throw std::exception();
  }
  if (cnt < k_) {
    history_list_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);

  } else {
    cache_list_.erase(cache_map_[frame_id]);
    cache_map_.erase(frame_id);
  }
  curr_size_--;
  access_count_[frame_id] = 0;
  is_evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
