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
#include "execution/executors/delete_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/table_heap.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() {
    child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    TableMetadata *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid()); 
    Tuple tmp_tuple;
    RID tmp_rid;
    if (!child_executor_->Next(&tmp_tuple, &tmp_rid)) {
        return false;
    }
    table_info->table_->MarkDelete(tmp_rid, exec_ctx_->GetTransaction());
    std::vector<IndexInfo*> IndexInfos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (const IndexInfo* index_info : IndexInfos) {
        index_info->index_->DeleteEntry(tmp_tuple, tmp_rid, exec_ctx_->GetTransaction());
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
