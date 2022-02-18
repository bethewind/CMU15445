//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdio>
#include <memory>

#include "catalog/catalog.h"
#include "common/exception.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  RID tmp_rid;
  if (!child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    return false;
  }
  // 进行加锁
  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  if (lock_mgr != nullptr && txn != nullptr && !txn->IsExclusiveLocked(tmp_rid)) {
    // 如果已经持有了共享锁，则直接进行升级
    bool lock_result = false;
    if (txn->IsSharedLocked(tmp_rid)) {
      lock_result = lock_mgr->LockUpgrade(txn, tmp_rid);
    } else {
      lock_result = lock_mgr->LockExclusive(txn, tmp_rid);
    }
    if (!lock_result) {
      throw Exception("LOCK FAIL");
    }
  }

  Tuple new_tuple = GenerateUpdatedTuple(tmp_tuple);
  bool update_result = table_info_->table_->UpdateTuple(new_tuple, tmp_rid, exec_ctx_->GetTransaction());
  if (!update_result) {
    throw Exception("UPDATE FAIL");
  }
  *rid = tmp_rid;
  return true;
}
}  // namespace bustub
