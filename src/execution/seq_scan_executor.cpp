//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "common/config.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cur_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
      end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::Init() {
  TableMetadata *table_meta_data = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  schema_ = &table_meta_data->schema_;
  cur_ = table_meta_data->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_meta_data->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (cur_ != end_) {
    if (plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(&*cur_, schema_).GetAs<bool>()) {
      break;
    }
    ++cur_;
  }
  if (cur_ == end_) {
    return false;
  }
  const Schema *out_schema = plan_->OutputSchema();
  std::vector<Value> values;
  const std::vector<Column> &columns = out_schema->GetColumns();
  for (const auto &column : columns) {
    const AbstractExpression *expr = column.GetExpr();
    values.push_back(expr->Evaluate(&*cur_, schema_));
  }
  *tuple = Tuple(values, out_schema);
  *rid = cur_->GetRid();
  ++cur_;
  return true;
}
}  // namespace bustub
