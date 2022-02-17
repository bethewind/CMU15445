//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <cxxabi.h>
#include <sys/wait.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <functional>
#include <mutex>  //NOLINT
#include <stack>
#include <type_traits>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "type/limits.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  IsolationLevel level = txn->GetIsolationLevel();
  if (level == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // 读提交和可重复读正常加锁
  txn->GetSharedLockSet()->emplace(rid);
  std::unique_lock<std::mutex> l(latch_);
  auto &req_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  LockRequest req(txn->GetTransactionId(), LockMode::SHARED);

  auto can_lock = [&]() {
    bool ok = true;
    for (const auto &req : req_queue) {
      if (req.granted_ && req.lock_mode_ == LockMode::EXCLUSIVE) {
        ok = false;
        break;
      }
    }
    return ok;
  };

  // 不管怎么样都要将这个请求放到队列中
  req.granted_ = can_lock();
  req_queue.emplace_back(req);
  // 不行的话就就行等待
  if (!req.granted_) {
    // 唤醒的两种可能，一种是自己被终止了，第二种是自己可以上锁了。
    txn_rid_[txn->GetTransactionId()] = rid;
    cv.wait(l, [&]() { return txn->GetState() == TransactionState::ABORTED || can_lock(); });
    txn_rid_.erase(txn->GetTransactionId());
  }

  auto get_req_iter = [&](txn_id_t txn_id) {
    std::remove_reference_t<decltype(req_queue)>::iterator cur;
    for (cur = req_queue.begin(); cur != req_queue.end(); ++cur) {
      if (cur->txn_id_ == txn_id) {
        break;
      }
    }
    return cur;
  };

  auto req_iter = get_req_iter(txn->GetTransactionId());
  // 自己醒来发现自己的状态变成ABORTED是有可能的。
  // 这是因为自己有可能被死锁检测给杀死，也就是把状态
  // 变成ABORTED
  if (txn->GetState() == TransactionState::ABORTED) {
    // 如果发现自己状态是Aborted，则首先将这个rid从自己的锁集合
    // 中删除，然后将自己从req_queue中剔除
    txn->GetSharedLockSet()->erase(rid);
    req_queue.erase(req_iter);
    // 如果还有等待的话，将他们唤醒
    cv.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;  // 返回false
  }

  // 自己获得了共享锁，确定自己的状态，并返回
  // 将自己的状态置为true，并返回
  req_iter->granted_ = true;
  assert(txn->GetState() == TransactionState::GROWING);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // LOG_INFO("Transaction: %d, RID: %d, %d LockExclusive, State is: %d", txn->GetTransactionId(), rid.GetPageId(),
  // rid.GetSlotNum(), static_cast<int>(txn->GetState()));
  // 对于所有的隔离级别，互斥锁都是正常枷锁
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  std::unique_lock<std::mutex> l(latch_);
  auto &req_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  LockRequest req(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  auto can_lock = [&]() {
    bool ok = true;
    for (const auto &req : req_queue) {
      if (req.granted_) {
        ok = false;
        break;
      }
    }
    return ok;
  };

  req.granted_ = can_lock();
  req_queue.emplace_back(req);
  if (!req.granted_) {
    txn_rid_[txn->GetTransactionId()] = rid;
    cv.wait(l, [&]() { return txn->GetState() == TransactionState::ABORTED || can_lock(); });
    txn_rid_.erase(txn->GetTransactionId());
  }

  auto get_req_iter = [&](txn_id_t txn_id) {
    std::remove_reference_t<decltype(req_queue)>::iterator cur;
    for (cur = req_queue.begin(); cur != req_queue.end(); ++cur) {
      if (cur->txn_id_ == txn_id) {
        break;
      }
    }
    return cur;
  };

  auto req_iter = get_req_iter(txn->GetTransactionId());

  if (txn->GetState() == TransactionState::ABORTED) {
    // 如果发现自己状态是Aborted，则首先将这个rid从自己的锁集合
    // 中删除，然后将自己从req_queue中剔除
    txn->GetExclusiveLockSet()->erase(rid);
    req_queue.erase(req_iter);
    // 如果还有等待的话，将他们唤醒
    cv.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;  // 返回false
  }
  // 将自己的状态置为true，并返回
  req_iter->granted_ = true;
  assert(txn->GetState() == TransactionState::GROWING);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // LOG_INFO("Transaction: %d, RID: %d, %d LockUpgrade, State is: %d", txn->GetTransactionId(), rid.GetPageId(),
  // rid.GetSlotNum(), static_cast<int>(txn->GetState()));
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  std::unique_lock<std::mutex> l(latch_);
  auto &req_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  auto &upgrading = lock_table_[rid].upgrading_;

  // 已经有人在等待upgradding，则将自己abort
  if (upgrading) {
    txn->SetState(TransactionState::ABORTED);
    // 要抛异常么？
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  auto get_req_iter = [&](txn_id_t txn_id) {
    std::remove_reference_t<decltype(req_queue)>::iterator cur;
    for (cur = req_queue.begin(); cur != req_queue.end(); ++cur) {
      if (cur->txn_id_ == txn_id) {
        break;
      }
    }
    return cur;
  };

  auto req_iter = get_req_iter(txn->GetTransactionId());
  // 确保自己之前持有
  assert(req_iter->granted_);
  // 修改类型
  req_iter->granted_ = false;
  req_iter->lock_mode_ = LockMode::EXCLUSIVE;

  // 查看是否可以直接upgrade
  auto can_upgrade = [&]() {
    bool ok = true;
    for (auto req : req_queue) {
      if (req.granted_) {
        ok = false;
        break;
      }
    }
    return ok;
  };

  bool ok = can_upgrade();

  // 不可以直接升级的话，就将自己等待在upgrading的条件变量中
  if (!ok) {
    upgrading = true;
    txn_rid_[txn->GetTransactionId()] = rid;
    cv.wait(l, [&]() { return txn->GetState() == TransactionState::ABORTED || can_upgrade(); });
    txn_rid_.erase(txn->GetTransactionId());
  }

  upgrading = false;

  if (txn->GetState() == TransactionState::ABORTED) {
    // 如果发现自己状态是Aborted，则首先将这个rid从自己的锁集合
    // 中删除
    txn->GetExclusiveLockSet()->erase(rid);
    req_queue.erase(req_iter);
    // 如果还有等待的话，将他们唤醒
    cv.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;  // 返回false
  }

  // 可以升级了, 修改自己的状态
  req_iter->granted_ = true;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // LOG_INFO("Transaction: %d, RID: %d, %d Unlock, State is: %d", txn->GetTransactionId(), rid.GetPageId(),
  // rid.GetSlotNum(), static_cast<int>(txn->GetState()));
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  std::unique_lock<std::mutex> l(latch_);
  auto &req_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  auto get_req_iter = [&](txn_id_t txn_id) {
    std::remove_reference_t<decltype(req_queue)>::iterator cur;
    for (cur = req_queue.begin(); cur != req_queue.end(); ++cur) {
      if (cur->txn_id_ == txn_id) {
        break;
      }
    }
    return cur;
  };

  auto req_iter = get_req_iter(txn->GetTransactionId());

  assert(req_iter->granted_);
  req_queue.erase(req_iter);
  cv.notify_all();

  // 只有之前状态是growing的时候才会变成SHRINKONG，
  // 因为有可能在ABORTED或者COMMIT的时候释放锁
  // 对于读提交的隔离级别，可以在unlock之后继续上锁
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &g = waits_for_[t1];
  auto iter = g.find(t2);
  if (iter == g.end()) {
    g.insert(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::unordered_set<txn_id_t> vis;
  std::stack<txn_id_t> stk;
  std::unordered_set<txn_id_t> in_stack;
  txn_id_t last_id{-1};
  bool find{false};
  std::function<bool(txn_id_t)> dfs = [&](txn_id_t cur) {
    vis.insert(cur);
    in_stack.insert(cur);
    stk.push(cur);
    for (auto x : waits_for_[cur]) {
      if (in_stack.find(x) != in_stack.end()) {
        last_id = x;
        find = true;
        return true;
      }
      if (vis.find(x) != vis.end()) {
        continue;
      }
      if (dfs(x)) {
        return true;
      }
    }
    in_stack.erase(cur);
    stk.pop();
    return false;
  };

  for (auto &x : waits_for_) {
    if (vis.find(x.first) == vis.end() && dfs(x.first)) {
      break;
    }
  }

  if (find) {
    txn_id_t youngest = last_id;
    while (!stk.empty() && stk.top() != last_id) {
      youngest = std::max(youngest, stk.top());
      stk.pop();
    }
    *txn_id = youngest;
    return true;
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> g;
  for (auto &[k, v] : waits_for_) {
    for (auto t : v) {
      g.emplace_back(k, t);
    }
  }
  return g;
}

void LockManager::RunCycleDetection() {
  auto build_graph = [&]() {
    waits_for_.clear();

    for (auto &t : lock_table_) {
      auto &req_queue = t.second.request_queue_;
      std::vector<txn_id_t> grant;
      std::vector<txn_id_t> wait;
      for (auto &req : req_queue) {
        txn_id_t txn_id = req.txn_id_;
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        if (txn->GetState() == TransactionState::ABORTED) {
          continue;
        }
        if (req.granted_) {
          grant.push_back(req.txn_id_);
        } else {
          wait.push_back(req.txn_id_);
        }
      }
      // 所有等待的事务向所有持有的事务连一条边
      for (auto x : wait) {
        for (auto y : grant) {
          AddEdge(x, y);
        }
      }
    }
  };

  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      build_graph();
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        RID rid = txn_rid_[txn_id];
        lock_table_[rid].cv_.notify_all();
        waits_for_.erase(txn_id);
        for (auto &x : waits_for_) {
          x.second.erase(txn_id);
        }
      }
    }
  }
}

}  // namespace bustub
