/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *begin_leaf_page, int cur_index,
                                  BufferPoolManager *buffer_pool_manager, bool is_end)
    : cur_leaf_page_(begin_leaf_page),
      cur_index_(cur_index),
      buffer_pool_manager_(buffer_pool_manager),
      is_end_(is_end) {
  // 边界情况，如果KeyIndex返回的最后一个非法的情况，则需要重新更新一下内部状态。
  // 比如一个B+树是稀疏的，然后按照内部节点找到了叶子节点1，但是传入的key又比
  // 内部所有的元素都大，也就是传入的key是B+树不存在的，而且刚好找到了一个叶子节点
  // 的最后一个位置
  if ((cur_leaf_page_ != nullptr) && cur_index_ == cur_leaf_page_->GetSize()) {
    --cur_index_;
    ++*this;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (!is_end_) {
    buffer_pool_manager_->UnpinPage(cur_leaf_page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return cur_leaf_page_->GetItem(cur_index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (is_end_) {
    return *this;
  }
  cur_index_++;
  if (cur_index_ == cur_leaf_page_->GetSize()) {
    page_id_t next_page_id = cur_leaf_page_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(cur_leaf_page_->GetPageId(), false);
    if (next_page_id == INVALID_PAGE_ID) {
      is_end_ = true;
    } else {
      Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
      cur_leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
      cur_index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
