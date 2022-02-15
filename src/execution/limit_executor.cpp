//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), cur_(0) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  Tuple tmp_tuple;
  RID tmp_rid;
  while (cur_ < plan_->GetOffset()) {
    child_executor_->Next(&tmp_tuple, &tmp_rid);
    ++cur_;
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (cur_ >= plan_->GetOffset() + plan_->GetLimit()) {
    return false;
  }
  ++cur_;
  return child_executor_->Next(tuple, rid);
}

}  // namespace bustub
