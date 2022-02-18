//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "catalog/catalog.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "execution/execution_engine.h"
#include "execution/executors/delete_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/page/table_page.h"
#include "storage/table/table_heap.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableMetadata *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  Tuple tmp_tuple;
  RID tmp_rid;
  if (!child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    return false;
  }
  bool delete_result = table_info->table_->MarkDelete(tmp_rid, exec_ctx_->GetTransaction());
  if (!delete_result) {
    throw Exception("DELETE FAIL");
  }
  std::vector<IndexInfo *> IndexInfos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  for (const IndexInfo *index_info : IndexInfos) {
    index_info->index_->DeleteEntry(tmp_tuple, tmp_rid, exec_ctx_->GetTransaction());
    IndexWriteRecord index_write_record{tmp_rid,   table_info->oid_,       WType::DELETE,
                                        tmp_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()};
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(index_write_record);
  }
  if (tuple != nullptr) {
    *tuple = tmp_tuple;
  }
  if (rid != nullptr) {
    *rid = tmp_rid;
  }
  return true;
}

}  // namespace bustub
