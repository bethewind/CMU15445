//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size == INTERNAL_PAGE_SIZE ? internal_max_size - 1 : internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  std::lock_guard<std::mutex> lk(latch_);
  if (IsEmpty()) {
    return false;
  }

  Page *leaf_page = FindLeafPage(key, false);
  if (leaf_page == nullptr) {
    return false;
  }
  LeafPage *leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  ValueType signal_result;
  bool ans = leaf_node->Lookup(key, &signal_result, comparator_);
  if (ans) {
    result->emplace_back(signal_result);
  }
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  std::lock_guard<std::mutex> lk(latch_);
  bool ans = true;
  if (IsEmpty()) {
    StartNewTree(key, value);
    // return true;
  }
  ans = InsertIntoLeaf(key, value, transaction);
  return ans;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id;
  Page *new_root_page = buffer_pool_manager_->NewPage(&root_page_id);
  if (new_root_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "StartNewTree: buffer pool manager out of memory!");
  }
  LeafPage *new_root = reinterpret_cast<LeafPage *>(new_root_page->GetData());
  new_root->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  new_root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id, true);
  root_page_id_ = root_page_id;
  UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  page_id_t cur_page_id = root_page_id_;
  LeafPage *find = nullptr;
  Page *cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
  if (cur_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoLeaf: out of memory!");
  }
  BPlusTreePage *cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  // BPlusTreePage *cur_page = reinterpret_cast<BPlusTreePage
  // *>(buffer_pool_manager_->FetchPage(cur_page_id)->GetData());
  while (!cur_node->IsLeafPage()) {
    InternalPage *cur_internal_node = reinterpret_cast<InternalPage *>(cur_node);
    page_id_t child_page_id = cur_internal_node->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    cur_page_id = child_page_id;
    cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
    if (cur_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoLeaf: out of memory!");
    }
    cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  }
  find = reinterpret_cast<LeafPage *>(cur_page);
  int old_size = find->GetSize();
  int new_size = find->Insert(key, value, comparator_);
  if (old_size == new_size) {
    // insert fail
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    return false;
  }
  // split.
  if (find->GetSize() >= find->GetMaxSize()) {
    auto new_node = Split(find);
    InsertIntoParent(static_cast<BPlusTreePage *>(find), new_node->KeyAt(0), static_cast<BPlusTreePage *>(new_node),
                     transaction);
  } else {
    buffer_pool_manager_->UnpinPage(cur_page_id, true);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  assert(new_page_id > 0);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Split: buffer pool manager out of memory!");
  }
  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    leaf_node->MoveHalfTo(new_leaf_node);
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_internal_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  // Cat InsertIntoParent
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  page_id_t parent_page_id = old_node->GetParentPageId();
  // 如果没有父节点，则需要新创建一个父节点
  if (parent_page_id == INVALID_PAGE_ID) {
    page_id_t new_root_page_id;
    Page *new_root = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (new_root == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoParent: buffer pool manager out of memory!");
    }
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(new_root->GetData());
    new_root_node->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
  } else {
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    if (parent_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoParent: buffer pool manager out of memory!");
    }
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    int parent_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    if (parent_size > parent_node->GetMaxSize()) {
      auto new_node = Split(parent_node);
      InsertIntoParent(static_cast<BPlusTreePage *>(parent_node), new_node->KeyAt(0),
                       static_cast<BPlusTreePage *>(new_node), nullptr);
    } else {
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::lock_guard<std::mutex> lk(latch_);
  if (IsEmpty()) {
    return;
  }

  Page *leaf_page = FindLeafPage(key, false);
  if (leaf_page == nullptr) {
    return;
  }

  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int old_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  if (old_size == new_size) {
    // key not exist.
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return;
  }
  if (new_size < leaf_node->GetMinSize()) {
    // transfer the leaf_node to CoalesceOrRedistribute() function.
    CoalesceOrRedistribute(leaf_node, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  page_id_t parent_page_id = node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    // 表示当前节点是根节点，则直接调用 AdjustRoot即可
    return AdjustRoot(node);
  }

  // get parent node
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "CoalesceOrRedistribute: out of memory!");
  }
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int node_index = parent_node->ValueIndex(node->GetPageId());

  // If it has left brother, we choose it. Otherwise,
  // we choose the right brother.
  int sibling_index = 0;
  if (node_index == 0) {
    sibling_index = node_index + 1;
  } else {
    sibling_index = node_index - 1;
  }

  // Get the sibling node
  Page *sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(sibling_index));
  if (sibling_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "CoalesceOrRedistribute: out of memory!");
  }
  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  bool delete_page = false;
  if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
    // redistribute
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
    Redistribute(sibling_node, node, node_index);

  } else {
    // merge
    if (node_index == 0) {
      Coalesce(&node, &sibling_node, &parent_node, 1, transaction);
    } else {
      Coalesce(&sibling_node, &node, &parent_node, node_index, transaction);
    }
    delete_page = true;
  }
  return delete_page;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
/**
 * 合并的时候会将父节点删除一个元素，可能会导致父节点的合并或者借节点
 * 所以这个函数可能需要对父节点递归调用 CoalesceOrRedistribute 函数。
 * 当需要进行合并的时候，总是将右边的兄弟合并到左边，并将父节点中右兄弟的<k, v>进行删除
 * 合并的时候只需要删除父节点中的内容，而不需要修改其中的内容
 * index 表示node在parent中的位置
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *leaf_neighbor_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_node->MoveAllTo(leaf_neighbor_node);
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *internal_neighbor_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    internal_node->MoveAllTo(internal_neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
  }
  (*parent)->Remove(index);
  // Release the buffer.
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(), false);
  buffer_pool_manager_->DeletePage((*node)->GetPageId());
  buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true);

  // the lifecycle of parent is maintained by CoalesceOrRedistribute() function.
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }

  buffer_pool_manager_->UnpinPage((*parent)->GetPageId(), true);
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
/**
 * 所有的移动操作都是从neighbor_node 到 node
 * index 表示 node 在 parent 中的index
 * 所以如果index是0，那么sibling就是node的右兄弟，所以要将sibling的第一个挪到
 * node的第一个，否则sibling就是node的左兄弟，要将sibling的最后一个挪到node的第一个
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // Get the Parent page because we need to modify the key of parent.
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  if (parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Redistribute: out of memory!");
  }
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *leaf_neighbor_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {
      leaf_neighbor_node->MoveFirstToEndOf(leaf_node);
      parent_node->SetKeyAt(1, leaf_neighbor_node->KeyAt(0));
    } else {
      leaf_neighbor_node->MoveLastToFrontOf(leaf_node);
      parent_node->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *internal_neighbor_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {
      KeyType middle_key = parent_node->KeyAt(1);
      internal_neighbor_node->MoveFirstToEndOf(internal_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(1, internal_neighbor_node->KeyAt(0));
    } else {
      KeyType middle_key = parent_node->KeyAt(index);
      internal_neighbor_node->MoveLastToFrontOf(internal_node, middle_key, buffer_pool_manager_);
      parent_node->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  // Release these nodes.
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
/**
 * 递归调用最终要处理的节点的是根节点，则可能有如下两种可能。
 * 1. 根节点只有一个孩子，那么树的高度要减一，然后更新root_page_id。
 * 2. 根节点没有孩子，而且根节点被删完了，那么需要将整棵树置空。
 *
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  bool delete_root_page = false;
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      // 第二种情况
      buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
      buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      delete_root_page = true;
    } else {
      buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
      delete_root_page = false;
    }
  } else {
    if (old_root_node->GetSize() == 1) {
      InternalPage *internal_old_root_node = reinterpret_cast<InternalPage *>(old_root_node);
      page_id_t new_root_page_id = internal_old_root_node->RemoveAndReturnOnlyChild();
      Page *new_root_page = buffer_pool_manager_->FetchPage(new_root_page_id);
      if (new_root_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "AdjustRoot: out of memory!");
      }
      BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
      new_root_node->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = new_root_page_id;
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
      buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
      buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
      delete_root_page = true;
    } else {
      buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
      delete_root_page = false;
    }
  }
  return delete_root_page;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  Page *leaf_page = FindLeafPage(KeyType{}, true);
  INDEXITERATOR_TYPE ans(nullptr, 0, nullptr, true);
  if (leaf_page != nullptr) {
    ans = INDEXITERATOR_TYPE(reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData()), 0,
                             buffer_pool_manager_, false);
  }
  return ans;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *leaf_page = FindLeafPage(key, false);
  INDEXITERATOR_TYPE ans(nullptr, 0, nullptr, true);

  if (leaf_page != nullptr) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    int key_index = leaf_node->KeyIndex(key, comparator_);
    if (key_index == -1) {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    } else {
      ans = INDEXITERATOR_TYPE(leaf_node, key_index, buffer_pool_manager_, false);
    }
  }
  return ans;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, 0, nullptr, true); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if (IsEmpty()) {
    return nullptr;
  }
  page_id_t cur_page_id = root_page_id_;
  Page *cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
  if (cur_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "FindLeafPage: out of memory!");
  }
  BPlusTreePage *cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  while (!cur_node->IsLeafPage()) {
    InternalPage *cur_internal_node = reinterpret_cast<InternalPage *>(cur_node);
    page_id_t child_page_id = INVALID_PAGE_ID;
    if (leftMost) {
      child_page_id = cur_internal_node->ValueAt(0);
    } else {
      child_page_id = cur_internal_node->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    cur_page_id = child_page_id;
    cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
    if (cur_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "FindLeafPage: out of memory!");
    }
    cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  }
  return cur_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
