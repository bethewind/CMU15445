//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <cassert>
#include <stdexcept>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/insert_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/generic_key.h"
#include "storage/index/index.h"
#include "storage/index/index_iterator.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_(nullptr),
      cur_(nullptr, 0, nullptr, true),
      table_metadata_(nullptr) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  index_ = index_info->index_.get();
  cur_ = dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_)->GetBeginIterator();
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  const AbstractExpression *expr = plan_->GetPredicate();
  const Schema *output_schema = plan_->OutputSchema();
  Tuple tmp_tuple;
  while (!cur_.isEnd()) {
    const RID &tmp_rid = (*cur_).second;
    bool find = table_metadata_->table_->GetTuple(tmp_rid, &tmp_tuple, exec_ctx_->GetTransaction());
    assert(find);
    if (expr->Evaluate(&tmp_tuple, &table_metadata_->schema_).GetAs<bool>()) {
      const std::vector<Column> columns = output_schema->GetColumns();
      std::vector<Value> values;
      values.reserve(columns.size());
      for (const auto &column : columns) {
        values.emplace_back(column.GetExpr()->Evaluate(&tmp_tuple, &table_metadata_->schema_));
      }
      *tuple = Tuple(values, output_schema);
      *rid = tmp_rid;
      ++cur_;
      return true;
    }
    ++cur_;
  }
  return false;
}

}  // namespace bustub
