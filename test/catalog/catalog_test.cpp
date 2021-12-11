//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "catalog/table_generator.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/generic_key.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  EXPECT_EQ(table_metadata->name_, table_name);
  EXPECT_EQ(table_metadata->oid_, 0);
  delete catalog;
  delete bpm;
  delete disk_manager;
}

TEST(CatalogTest, CreateIndexTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  auto txn = new Transaction(0);
  auto exec_ctx = new ExecutorContext(txn, catalog, bpm, nullptr, nullptr);

  auto table_genetor = TableGenerator(exec_ctx);

  table_genetor.GenerateTestTables();

  std::vector<std::string> table_names = {"empty_table", "test_1", "test_2", "test_3", "empty_table2", "empty_table3"};

  for (const auto &table_name : table_names) {
    ASSERT_TRUE(catalog->GetTable(table_name) != nullptr);
  }

  TableMetadata *table_metadata_for_test1 = catalog->GetTable("test_3");
  std::vector<uint32_t> key_attrs = {0};
  Schema *index_scheme = Schema::CopySchema(&(table_metadata_for_test1->schema_), key_attrs);
  IndexInfo *index_info = catalog->CreateIndex<GenericKey<4>, RID, GenericComparator<4>>(
      txn, "index_1", "test_3", table_metadata_for_test1->schema_, *index_scheme, key_attrs, 4);
  EXPECT_TRUE(index_info != nullptr);
  EXPECT_EQ(index_info->index_oid_, 0);
  auto index = static_cast<BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>> *>(index_info->index_.get());
  auto index_iter = index->GetBeginIterator();

  uint32_t counter = 0;
  while (index_iter != index->GetEndIterator()) {
    auto key = (*index_iter).first;

    uint32_t key_int = key.ToValue(index_scheme, 0).GetAs<uint32_t>();
    EXPECT_EQ(key_int, counter);
    counter += 1;
    ++index_iter;
  }

  delete index_scheme;
  delete exec_ctx;
  delete txn;
  delete catalog;
  delete bpm;
  delete disk_manager;
}
}  // namespace bustub
