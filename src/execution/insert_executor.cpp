//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/table_heap.h"
#include "type/abstract_pool.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), cur_index_(0) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableMetadata *table_metadata = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  const Schema *insert_schema = &table_metadata->schema_;
  const std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata->name_);
  Tuple tmp_tuple;
  RID tmp_rid;
  if (plan_->IsRawInsert()) {
    if (cur_index_ >= plan_->RawValues().size()) {
      return false;
    }
    tmp_tuple = Tuple(plan_->RawValuesAt(cur_index_), insert_schema);
    ++cur_index_;
  } else {
    if (!child_executor_->Next(&tmp_tuple, &tmp_rid)) {
      return false;
    }
  }
  table_metadata->table_->InsertTuple(tmp_tuple, &tmp_rid, exec_ctx_->GetTransaction());
  for (const IndexInfo *index_info : index_infos) {
    index_info->index_->InsertEntry(tmp_tuple, tmp_rid, exec_ctx_->GetTransaction());
  }

  *tuple = tmp_tuple;
  *rid = tmp_rid;
  return true;
}

}  // namespace bustub
