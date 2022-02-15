//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <cstdio>
#include "catalog/catalog.h"
#include "common/logger.h"

namespace bustub {
// 这个题目设计应该是有问题的!
NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple outter_tuple;
  RID outter_rid;
  RID inner_rid;
  LOG_INFO("================");
  const std::string &table_name = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->name_;
  LOG_INFO("Inner Table Name: %s", table_name.c_str());

  Index *inner_table_index = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), table_name)->index_.get();
  LOG_INFO("Index name: %s", inner_table_index->GetName().c_str());
  const std::vector<Column> &inner_table_index_columns =
      exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), table_name)->key_schema_.GetColumns();

  // DEBUG
  for (const auto &column : inner_table_index_columns) {
    LOG_INFO("Index Column name: %s", column.GetName().c_str());
  }
  // DEBUG

  const std::vector<Column> &outer_table_columns = plan_->OuterTableSchema()->GetColumns();
  for (const auto &column : outer_table_columns) {
    LOG_INFO("Outer table Column name: %s", column.GetName().c_str());
  }

  LOG_INFO("================");
  while (true) {
    if (!child_executor_->Next(&outter_tuple, &outter_rid)) {
      return false;
    }
    Tuple inner_index_search_tuple = outter_tuple.KeyFromTuple(
        *plan_->OuterTableSchema(), *inner_table_index->GetKeySchema(), inner_table_index->GetKeyAttrs());
    std::vector<RID> result;
    inner_table_index->ScanKey(inner_index_search_tuple, &result, exec_ctx_->GetTransaction());
    if (!result.empty()) {
      assert(result.size() == 1);
      inner_rid = result[0];
      break;
    }
  }
  Tuple inner_tuple;
  exec_ctx_->GetCatalog()->GetTable(table_name)->table_->GetTuple(inner_rid, &inner_tuple, exec_ctx_->GetTransaction());
  const std::vector<Column> &columns = plan_->OutputSchema()->GetColumns();
  std::vector<Value> values;
  values.reserve(columns.size());
  for (const Column &column : columns) {
    values.emplace_back(column.GetExpr()->EvaluateJoin(&outter_tuple, plan_->OuterTableSchema(), &inner_tuple,
                                                       plan_->InnerTableSchema()));
  }
  const Schema *out_schema = plan_->OutputSchema();
  *tuple = Tuple(values, out_schema);
  return true;
}

}  // namespace bustub
