//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      stop_(false) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  stop_ = !left_executor_->Next(&left_tuple_, &left_rid_);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (stop_) {
    return false;
  }
  while (true) {
    if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        stop_ = true;
        return false;
      }
      right_executor_->Init();
    } else {
      if (plan_->Predicate()
              ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                             right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        break;
      }
    }
  }

  const std::vector<Column> &columns = plan_->OutputSchema()->GetColumns();
  std::vector<Value> values;
  values.reserve(columns.size());
  for (const Column &column : columns) {
    values.emplace_back(column.GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                       right_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  return true;
}

}  // namespace bustub
