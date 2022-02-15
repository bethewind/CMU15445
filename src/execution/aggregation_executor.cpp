//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "type/abstract_pool.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tmp_tuple;
  RID tmp_rid;
  while (child_->Next(&tmp_tuple, &tmp_rid)) {
    AggregateKey agg_key = MakeKey(&tmp_tuple);
    AggregateValue agg_value = MakeVal(&tmp_tuple);
    aht_.InsertCombine(agg_key, agg_value);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  const AbstractExpression *having = plan_->GetHaving();
  while (aht_iterator_ != aht_.End()) {
    const std::vector<Value> &group_bys = aht_iterator_.Key().group_bys_;
    const std::vector<Value> &aggregates = aht_iterator_.Val().aggregates_;
    if (having != nullptr && !plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()) {
      ++aht_iterator_;
      continue;
    }
    std::vector<Value> values;
    const std::vector<Column> columns = plan_->OutputSchema()->GetColumns();
    values.reserve(columns.size());
    for (const Column &column : columns) {
      values.emplace_back(column.GetExpr()->EvaluateAggregate(group_bys, aggregates));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub
