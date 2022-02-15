#pragma once

#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/page/table_page.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * Metadata about a index
 */
struct IndexInfo {
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_(std::move(key_schema)),
        name_(std::move(name)),
        index_(std::move(index)),
        index_oid_(index_oid),
        table_name_(std::move(table_name)),
        key_size_(key_size) {}
  Schema key_schema_;
  std::string name_;
  std::unique_ptr<Index> index_;
  index_oid_t index_oid_;
  std::string table_name_;
  const size_t key_size_;
};

/**
 * Catalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class Catalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");
    // get table_oid;
    table_oid_t table_oid = next_table_oid_++;
    // create table heap
    std::unique_ptr<TableHeap> table_heap = std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn);
    // create table metadata
    std::unique_ptr<TableMetadata> table_metadata =
        std::make_unique<TableMetadata>(schema, table_name, std::move(table_heap), table_oid);
    TableMetadata *ans = table_metadata.get();
    // maintain the data structure.
    names_[table_name] = table_oid;
    tables_[table_oid] = std::move(table_metadata);
    return ans;
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) {
    auto names_iter = names_.find(table_name);
    if (names_iter == names_.end()) {
      throw std::out_of_range("Cannot find table");
    }
    table_oid_t table_oid = names_iter->second;
    return GetTable(table_oid);
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    auto tables_iter = tables_.find(table_oid);
    if (tables_iter == tables_.end()) {
      throw std::out_of_range("Cannot find table");
    }
    return tables_iter->second.get();
  }
  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param index_name the name of the new index
   * @param table_name the name of the table
   * @param schema the schema of the table
   * @param key_schema the schema of the key
   * @param key_attrs key attributes
   * @param keysize size of the key
   * @return a pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  IndexInfo *CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
                         const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
                         size_t keysize) {
    // acquire the index_oid
    index_oid_t index_oid = next_index_oid_++;
    // create the index
    IndexMetadata *index_metadata = new IndexMetadata(index_name, table_name, &schema, key_attrs);
    std::unique_ptr<Index> index =
        std::make_unique<BPlusTreeIndex<KeyType, ValueType, KeyComparator>>(index_metadata, bpm_);
    // polulate existing data of the table.
    TableHeap *table_heap = GetTable(table_name)->table_.get();
    std::uint32_t counter = 0;
    for (auto iter = table_heap->Begin(txn); iter != table_heap->End(); ++iter) {
      Tuple tuple = iter->KeyFromTuple(schema, key_schema, key_attrs);
      index->InsertEntry(tuple, iter->GetRid(), txn);
      ++counter;
    }
    LOG_INFO("Insert %d element to index %s of table %s", counter, index_name.c_str(), table_name.c_str());
    for (auto x : key_attrs) {
      LOG_INFO("Key attributes: %d", x);
    }
    const std::vector<Column> &columns = key_schema.GetColumns();
    for (const auto &column : columns) {
      LOG_INFO("Key Column name: %s", column.GetName().c_str());
    }

    const std::vector<Column> &columns_table = schema.GetColumns();
    for (const auto &column : columns_table) {
      LOG_INFO("Table Column name: %s", column.GetName().c_str());
    }

    std::unique_ptr<IndexInfo> index_info =
        std::make_unique<IndexInfo>(key_schema, index_name, std::move(index), index_oid, table_name, keysize);
    IndexInfo *ans = index_info.get();
    // maintain the data structure
    indexes_[index_oid] = std::move(index_info);
    index_names_[table_name][index_name] = index_oid;
    return ans;
  }

  IndexInfo *GetIndex(const std::string &index_name, const std::string &table_name) {
    auto index_names_iter = index_names_.find(table_name);
    if (index_names_iter == index_names_.end()) {
      throw std::out_of_range("Cannot find index");
    }
    auto table_indexs = index_names_iter->second;
    auto table_indexs_iter = table_indexs.find(index_name);
    if (table_indexs_iter == table_indexs.end()) {
      throw std::out_of_range("Cannot find index");
    }
    return GetIndex(table_indexs_iter->second);
  }

  IndexInfo *GetIndex(index_oid_t index_oid) {
    auto indexes_iter = indexes_.find(index_oid);
    if (indexes_iter == indexes_.end()) {
      throw std::out_of_range("Cannot find index");
    }
    return indexes_iter->second.get();
  }

  std::vector<IndexInfo *> GetTableIndexes(const std::string &table_name) {
    std::vector<IndexInfo *> ans;
    auto index_names_inter = index_names_.find(table_name);
    if (index_names_inter == index_names_.end()) {
      return ans;
    }
    for (auto &&ele : index_names_inter->second) {
      IndexInfo *get_result = GetIndex(ele.second);
      if (get_result != nullptr) {
        ans.push_back(get_result);
      }
    }
    return ans;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};
  /** indexes_: index identifiers -> index metadata. Note that indexes_ owns all index metadata */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;
  /** index_names_: table name -> index names -> index identifiers */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;
  /** The next index identifier to be used */
  std::atomic<index_oid_t> next_index_oid_{0};
};
}  // namespace bustub
