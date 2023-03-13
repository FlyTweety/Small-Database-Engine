#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include<iostream>
using namespace std;

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}


BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    //cout << "page_table_: "<< page.first << " " << page.second << endl;
    FlushPage(page.first);
  }
  //delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  latch_.lock();
  frame_id_t frame_id;
  //Search the page table for the requested page (P).
  if(page_table_.find(page_id) != page_table_.end()){// P exists
    frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
     latch_.unlock();
    return &pages_[frame_id];//pin it and return it immediately
  }
  if(free_list_.size()>0){// P does not exist, find a replacement page from free list
    frame_id = free_list_.front();
    page_table_[page_id] = frame_id;
    free_list_.pop_front();
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].page_id_ = page_id;
    latch_.unlock();
    return &pages_[frame_id];
  }
  else{// P does not exist, find a replacement page from free replacer
    bool valid = replacer_->Victim(&frame_id);
    if(valid == false) return nullptr;
    if(pages_[frame_id].IsDirty()){//脏页写回
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    page_table_[page_id] = frame_id;//Update P's metadata
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    latch_.unlock();
    return &pages_[frame_id];
  }
  latch_.unlock();
  return nullptr;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  latch_.lock();
  page_id = 0;
  frame_id_t frame_id;
  if(free_list_.size()>0){//Pick a victim page P from the free list
    page_id = AllocatePage();
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else{//Pick a victim page P from either the replacer
    bool valid = replacer_->Victim(&frame_id);
    if(valid == false) return nullptr;
    page_id = AllocatePage();
    if(pages_[frame_id].IsDirty()){//脏页写回
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    page_table_.erase(pages_[frame_id].page_id_);
  }
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  page_table_.insert({page_id, frame_id});
  latch_.unlock();

  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {//If P does not exist, return true.
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {//If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    latch_.unlock();
    return false;
  }
  page_table_.erase(page_id);//Remove P from the page table
  if (pages_[frame_id].is_dirty_) {//脏页的话要写回内存
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;//reset page's metadata
  free_list_.push_back(frame_id);//return the page to the free list
  DeallocatePage(page_id);
  latch_.unlock();

  return true;
}


bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {//page中找不到直接返回
      latch_.unlock();
      return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ == 0) {//当在一个数据页的引用计数变为0时调用
      return true;
  }
  if (pages_[frame_id].pin_count_ > 0) {//固定数减1
      pages_[frame_id].pin_count_ -= 1;
      replacer_->Unpin(frame_id);
  }
  if (is_dirty) pages_[frame_id].is_dirty_ = is_dirty;
  latch_.unlock();

  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {//page_id 无效或者page中找不到
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  latch_.unlock();
  return true;
}


page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}

