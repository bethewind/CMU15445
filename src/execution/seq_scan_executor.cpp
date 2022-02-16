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
#include "catalog/schema.h"
#include "common/config.h"
#include "common/logger.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), cur_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::Init() {
  cur_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  TableIterator end = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End();
  Schema cur_table_schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
  while (cur_ != end) {
    if (plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(&*cur_, &cur_table_schema).GetAs<bool>()) {
      break;
    }
    ++cur_;
  }
  if (cur_ == end) {
    return false;
  }
  const Schema *out_schema = plan_->OutputSchema();
  std::vector<Value> values;
  const std::vector<Column> &columns = out_schema->GetColumns();

  for (const auto &column : columns) {
    const AbstractExpression *expr = column.GetExpr();
    values.push_back(expr->Evaluate(&*cur_, &cur_table_schema));
  }
  *tuple = Tuple(values, out_schema);
  *rid = cur_->GetRid();
  ++cur_;
  return true;
}
}  // namespace bustub
