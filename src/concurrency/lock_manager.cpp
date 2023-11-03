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
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "type/type.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
  /**
   * grant a lock or upgrade, txn may:
   * (1) hold same lock
   * (2) doesn't hold any lock, grant
   * (3) hold a different lock, upgrade
   */  
  bool hold_lock_flag = false;
  LockMode cur_lock_mode = LockMode::EXCLUSIVE;
  auto check_table_lock = [&](LockMode check_lock_mode) -> bool {
    switch (check_lock_mode) {
      case LockMode::EXCLUSIVE:
        return (hold_lock_flag = txn->IsTableExclusiveLocked(oid));
      case LockMode::INTENTION_EXCLUSIVE:
        return (hold_lock_flag = txn->IsTableIntentionExclusiveLocked(oid));
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return (hold_lock_flag = txn->IsTableSharedIntentionExclusiveLocked(oid));
      case LockMode::SHARED:
        return (hold_lock_flag = txn->IsTableSharedLocked(oid));
      case LockMode::INTENTION_SHARED:
        return (hold_lock_flag = txn->IsTableIntentionSharedLocked(oid));
    }
    return false;
  };

  for (auto mode = LockMode::SHARED; mode <= LockMode::SHARED_INTENTION_EXCLUSIVE; mode = static_cast<LockMode>(static_cast<int>(mode) + 1)) {
    if (check_table_lock(mode)) {
      cur_lock_mode = mode;
      break;
    }
  }
  
  auto check_upgrade_lock= [&](LockMode check_lock_mode) -> bool {
    switch (check_lock_mode) {
      case LockMode::EXCLUSIVE:
        return false; // no upgrade lock available
      case LockMode::INTENTION_EXCLUSIVE:
        return (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE);
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return (lock_mode == LockMode::EXCLUSIVE);
      case LockMode::SHARED:
        return (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE);
      case LockMode::INTENTION_SHARED:
        return (lock_mode != LockMode::INTENTION_SHARED);  // any lock(other than S) should work, still check here
    }
    return false;
  };

  if (hold_lock_flag) {
    //LOG_INFO("txn%d reaches here, try upgrading lock, lock_mode = %d", txn->GetTransactionId(), (int)lock_mode);

    // case (1) 
    if (cur_lock_mode == lock_mode) {
      return true;
    }

    // case (3) upgrade
    bool upgrade_flag = check_upgrade_lock(cur_lock_mode);
    if (!upgrade_flag) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // 2PL check (for different isolation levels)
    CheckLockRequest(txn, lock_mode);
    
    // get map lock then get table's request queue lock then release map lock
    std::unique_lock<std::mutex> table_lock(this->table_lock_map_latch_);
    auto lock_queue_ptr = table_lock_map_[oid];
    std::unique_lock<std::mutex> request_queue_lock(lock_queue_ptr->latch_);
    table_lock.unlock();

    if (lock_queue_ptr->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      // this design is simply for simplicity
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    this->table_lock_map_[oid]->upgrading_ = txn->GetTransactionId();
    // TODO: keep the original request or not??
    // prioritize this upgrade request
    // remove original lock and insert new lock request to head
    for (auto iter = lock_queue_ptr->request_queue_.begin(); iter != lock_queue_ptr->request_queue_.end(); ++iter) {
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        lock_queue_ptr->request_queue_.erase(iter);
        // Remove original lock from txn's table lock set
        switch (cur_lock_mode) {
          case LockMode::EXCLUSIVE:
            txn->GetExclusiveTableLockSet()->erase(oid);
            break;
          case LockMode::INTENTION_EXCLUSIVE:
            txn->GetIntentionExclusiveTableLockSet()->erase(oid);
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
            break;
          case LockMode::SHARED:
            txn->GetSharedTableLockSet()->erase(oid);
            break;
          case LockMode::INTENTION_SHARED:
            txn->GetIntentionSharedTableLockSet()->erase(oid);
            break;  
        }
        break;
      }
    }
    lock_queue_ptr->request_queue_.push_back(
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
    const auto request_ptr = lock_queue_ptr->request_queue_.back();

    // producer consumer model
    bool grant_flag = false;
    while (!(grant_flag = TryGrantLock(txn, lock_mode, lock_queue_ptr))) {
      lock_queue_ptr->cv_.wait(request_queue_lock);
      if (txn->GetState() == TransactionState::ABORTED) {
        // wakeup other threads TODO: do we need to remove requests? Of course
        lock_queue_ptr->request_queue_.remove(request_ptr);
        lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
        lock_queue_ptr->cv_.notify_all();
        return false;
      }
      // wakeup other blocking threads 
       lock_queue_ptr->cv_.notify_all();
    }
    // upgrade success, update request lock status to true and set upgrading_ to -1
    request_ptr->granted_ = true;
    lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
    request_queue_lock.unlock();
  }
  // case (2) grant
  else {
    // 2PL check (for different isolation levels)
    CheckLockRequest(txn, lock_mode);

    //LOG_INFO("txn%d reaches here, try granting lock, try getting table map[ lock]", txn->GetTransactionId());

    std::unique_lock<std::mutex> table_lock(this->table_lock_map_latch_);
    // initialize queue
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      table_lock_map_.insert({oid, std::make_shared<LockRequestQueue>()});
    }
    auto lock_queue_ptr = table_lock_map_[oid];
    std::unique_lock<std::mutex> request_queue_lock(lock_queue_ptr->latch_);
    table_lock.unlock();

    //LOG_INFO("txn%d reaches here, got table map lock", txn->GetTransactionId());

    // enqueue the lock request in FIFO manner
    lock_queue_ptr->request_queue_.push_back(
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
    const auto request_ptr = lock_queue_ptr->request_queue_.back();

    bool grant_flag = false;
    while (!(grant_flag = TryGrantLock(txn, lock_mode, lock_queue_ptr))) {
      //LOG_INFO("txn%d blocking", txn->GetTransactionId());
      lock_queue_ptr->cv_.wait(request_queue_lock);
      //LOG_INFO("txn%d waken up", txn->GetTransactionId());
      if (txn->GetState() == TransactionState::ABORTED) {
        // wakeup other threads TODO: do we need to remove this request from the queue? Of course
        //LOG_INFO("txn%d Abort, notify other threads", txn->GetTransactionId());
        lock_queue_ptr->request_queue_.remove(request_ptr);
        lock_queue_ptr->cv_.notify_all();
        return false;
      }
      // wakeup other blocking threads 
       lock_queue_ptr->cv_.notify_all();
    }
    // grant success, update request status 
    assert(request_ptr->txn_id_ == txn->GetTransactionId());
    request_ptr->granted_ = true;
    request_queue_lock.unlock();
  }

  // grant success, update txn lock set
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;  
  }

  return true; 
}

/**auto LockManager::RemoveTableLock(Transaction *txn) -> void {

}*/

auto LockManager::TryGrantLock(Transaction *txn, LockMode lock_mode, const std::shared_ptr<LockRequestQueue> &lock_queue_ptr) -> bool {
  // priority: upgrade then fifo
  if (lock_queue_ptr->upgrading_ != INVALID_TXN_ID) {
    if (txn->GetTransactionId() != lock_queue_ptr->upgrading_) {
      return false;
    }
    else {
      // check all granted locks compatability with upgrading txn 
      // because we push request back to the queue, thus we only need to check requests before it
      for (auto iter = lock_queue_ptr->request_queue_.begin(); iter != lock_queue_ptr->request_queue_.end(); iter++) {
        if ((*iter)->txn_id_ == txn->GetTransactionId()) {
          return true;
        }
        if ((*iter)->granted_ && !IsCompatible(lock_mode, (*iter)->lock_mode_)) {
          return false;
        }
      }
    }
  }

  // now no upgrading txn
  // wanted lock needs to be compatabile with all request locks before it
  for (auto iter = lock_queue_ptr->request_queue_.begin(); iter != lock_queue_ptr->request_queue_.end(); iter++) {
    
    // check compatability and previous requests granted or not
    if ((*iter)->txn_id_ != txn->GetTransactionId()) {
      // previous request hasn't been permitted
      if (!(*iter)->granted_) {
        return false;
      }
      if (!IsCompatible(lock_mode, (*iter)->lock_mode_)) {
        //LOG_INFO("return false r_mode = %d", (int)(*iter)->lock_mode_);
        return false;
      }
    }
    else {
      break;
    }
  }

  return true;
}

auto LockManager::IsCompatible(LockMode l_mode, LockMode r_mode) -> bool {
  switch(l_mode) {
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_EXCLUSIVE:
      return (r_mode == LockMode::INTENTION_EXCLUSIVE) || (r_mode == LockMode::INTENTION_SHARED);
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return (r_mode == LockMode::INTENTION_SHARED);
    case LockMode::SHARED:
      return (r_mode == LockMode::SHARED) || (r_mode == LockMode::INTENTION_SHARED);
    case LockMode::INTENTION_SHARED:
      return (r_mode != LockMode::EXCLUSIVE);
  }
  return false;
}

auto LockManager::CheckLockRequest(Transaction *txn, LockMode lock_mode) -> void {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if ((txn->GetState() == TransactionState::SHRINKING) &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)
      ) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  // check whether txn holds a lock for the table
  bool hold_lock_flag = false;
  LockMode cur_lock_mode = LockMode::EXCLUSIVE;
  auto check_table_lock = [&](LockMode check_lock_mode) -> bool {
    switch (check_lock_mode) {
      case LockMode::EXCLUSIVE:
        return (hold_lock_flag = txn->IsTableExclusiveLocked(oid));
      case LockMode::INTENTION_EXCLUSIVE:
        return (hold_lock_flag = txn->IsTableIntentionExclusiveLocked(oid));
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return (hold_lock_flag = txn->IsTableSharedIntentionExclusiveLocked(oid));
      case LockMode::SHARED:
        return (hold_lock_flag = txn->IsTableSharedLocked(oid));
      case LockMode::INTENTION_SHARED:
        return (hold_lock_flag = txn->IsTableIntentionSharedLocked(oid));
    }
    return false;
  };

  for (auto mode = LockMode::SHARED; mode <= LockMode::SHARED_INTENTION_EXCLUSIVE; mode = static_cast<LockMode>(static_cast<int>(mode) + 1)) {
    if (check_table_lock(mode)) {
      cur_lock_mode = mode;
      break;
    }
  }

  if (!hold_lock_flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // now txn holds the lock
  // first check whether all rows of the table are unlocked for "txn"
  if ((txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
      !((*txn->GetSharedRowLockSet())[oid].empty())) ||
      (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() &&
      !((*txn->GetExclusiveRowLockSet())[oid].empty())))
  {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // now good for unlocking table
  // unlock according to different isolation levels then notify blocking threads
  // get request queue lock, because unlocking will remove request from the queue
  std::unique_lock<std::mutex> table_lock(this->table_lock_map_latch_);
  auto lock_queue_ptr = table_lock_map_[oid];
  std::unique_lock<std::mutex> request_queue_lock(lock_queue_ptr->latch_);
  table_lock.unlock();

  if (txn->GetState() == TransactionState::GROWING) {
    switch(txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        if (cur_lock_mode == LockMode::EXCLUSIVE || cur_lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
        if (cur_lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
  // modify lock queue state and txn state (remove the lock)
  for (const auto &iter : lock_queue_ptr->request_queue_) {
    if ((*iter).txn_id_ == txn->GetTransactionId()) {
      lock_queue_ptr->request_queue_.remove(iter);
      break;
    }
  }
  switch (cur_lock_mode) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
  }
  // txn->GetExclusiveTableLockSet()->erase(oid);

  // notify blocking threads
  lock_queue_ptr->cv_.notify_all();

  return true; 
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // similar to LockTable(), differences:
  // only S and X locks are permitted; 
  // hierarchy locking, txn should already hold "corresponding lock";
  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) &&
        !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid))
    {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableExclusiveLocked(oid) &&
        !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
        !txn->IsTableIntentionSharedLocked(oid) &&
        !txn->IsTableSharedLocked(oid))
    {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  assert(lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE);

  // TODO: put this part in one single method
  bool hold_lock_flag = false;
  LockMode cur_lock_mode = LockMode::EXCLUSIVE;
  auto check_row_lock = [&](LockMode check_lock_mode) -> bool {
    switch (check_lock_mode) {
      case LockMode::EXCLUSIVE:
        return (hold_lock_flag = txn->IsRowExclusiveLocked(oid, rid));
      case LockMode::SHARED:
        return (hold_lock_flag = txn->IsRowSharedLocked(oid, rid));
      default:
        return false;
    }
    return false;
  };

  for (auto mode = LockMode::SHARED; mode <= LockMode::SHARED_INTENTION_EXCLUSIVE; mode = static_cast<LockMode>(static_cast<int>(mode) + 1)) {
    if (check_row_lock(mode)) {
      cur_lock_mode = mode;
      break;
    }
  }

  assert(cur_lock_mode == LockMode::SHARED || cur_lock_mode == LockMode::EXCLUSIVE);
  
  auto check_upgrade_lock= [&](LockMode check_lock_mode) -> bool {
    switch (check_lock_mode) {
      case LockMode::SHARED:
        return (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE);
      default:
        return false;
    }
    return false;
  };

  if (hold_lock_flag) {
    // case (1) 
    if (cur_lock_mode == lock_mode) {
      return true;
    }

    // case (3) upgrade
    bool upgrade_flag = check_upgrade_lock(cur_lock_mode);
    if (!upgrade_flag) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // 2PL check (for different isolation levels)
    CheckLockRequest(txn, lock_mode);
    
    // get map lock then get row's request queue lock then release map lock
    std::unique_lock<std::mutex> row_lock(this->row_lock_map_latch_);
    auto lock_queue_ptr = row_lock_map_[rid];
    std::unique_lock<std::mutex> request_queue_lock(lock_queue_ptr->latch_);
    row_lock.unlock();

    if (lock_queue_ptr->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      // this design is simply for simplicity
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    this->row_lock_map_[rid]->upgrading_ = txn->GetTransactionId();

    // prioritize this upgrade request
    // remove original lock and insert new lock request to head
    for (auto iter = lock_queue_ptr->request_queue_.begin(); iter != lock_queue_ptr->request_queue_.end(); ++iter) {
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        lock_queue_ptr->request_queue_.erase(iter);
        // Remove original lock from txn's row lock set
        switch (cur_lock_mode) {
          case LockMode::EXCLUSIVE:
            (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
            break;
          case LockMode::SHARED:
            (*txn->GetSharedRowLockSet())[oid].erase(rid);
            break;
          default:
            break;
        }
        break;
      }
    }
    lock_queue_ptr->request_queue_.push_back(
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
    const auto request_ptr = lock_queue_ptr->request_queue_.back();

    // producer consumer model
    bool grant_flag = false;
    while (!(grant_flag = TryGrantLock(txn, lock_mode, lock_queue_ptr))) {
      lock_queue_ptr->cv_.wait(request_queue_lock);
      if (txn->GetState() == TransactionState::ABORTED) {
        // wakeup other threads TODO: do we need to remove requests???
        lock_queue_ptr->request_queue_.remove(request_ptr);
        lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
        lock_queue_ptr->cv_.notify_all();
        return false;
      }
      // wakeup other blocking threads 
      lock_queue_ptr->cv_.notify_all();
    }
    // upgrade success
    request_ptr->granted_ = true;
    lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
    request_queue_lock.unlock();
  }
  // case (2) grant
  else {
    // 2PL check (for different isolation levels)
    CheckLockRequest(txn, lock_mode);

    std::unique_lock<std::mutex> row_lock(this->row_lock_map_latch_);
    // initialize queue
    if (row_lock_map_.find(rid) == row_lock_map_.end()) {
      row_lock_map_.insert({rid, std::make_shared<LockRequestQueue>()});
    }
    auto lock_queue_ptr = row_lock_map_[rid];
    std::unique_lock<std::mutex> request_queue_lock(lock_queue_ptr->latch_);
    row_lock.unlock();

    // enqueue the lock request in FIFO manner
    lock_queue_ptr->request_queue_.push_back(
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
    const auto request_ptr = lock_queue_ptr->request_queue_.back();

    bool grant_flag = false;
    while (!(grant_flag = TryGrantLock(txn, lock_mode, lock_queue_ptr))) {
      lock_queue_ptr->cv_.wait(request_queue_lock);
      if (txn->GetState() == TransactionState::ABORTED) {
        // wakeup other threads TODO: do we need to remove requests from the queue???
        lock_queue_ptr->request_queue_.remove(request_ptr);
        lock_queue_ptr->cv_.notify_all();
        return false;
      }
      // // wakeup other blocking threads 
       lock_queue_ptr->cv_.notify_all();
    }
    request_ptr->granted_ = true;
    request_queue_lock.unlock();
  }

  // grant success, update txn lock set
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
      break;
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].insert(rid);
      break;
    default:
      break;
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  // check whether txn holds a lock for the row
  bool hold_lock_flag = false;
  LockMode cur_lock_mode = LockMode::EXCLUSIVE;
  auto check_row_lock = [&](LockMode check_lock_mode) -> bool {
    switch (check_lock_mode) {
      case LockMode::EXCLUSIVE:
        return (hold_lock_flag = txn->IsRowExclusiveLocked(oid, rid));
      case LockMode::SHARED:
        return (hold_lock_flag = txn->IsRowSharedLocked(oid, rid));
      default:
        return false;
    }
    return false;
  };

  for (auto mode = LockMode::SHARED; mode <= LockMode::SHARED_INTENTION_EXCLUSIVE; mode = static_cast<LockMode>(static_cast<int>(mode) + 1)) {
    if (check_row_lock(mode)) {
      cur_lock_mode = mode;
      break;
    }
  }

  if (!hold_lock_flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // now txn holds the lock
  // unlock according to different isolation levels then notify blocking threads
  // get request queue lock, because unlocking will remove request from the queue
  std::unique_lock<std::mutex> row_lock(this->row_lock_map_latch_);
  auto lock_queue_ptr = row_lock_map_[rid];
  std::unique_lock<std::mutex> request_queue_lock(lock_queue_ptr->latch_);
  row_lock.unlock();

  if (txn->GetState() == TransactionState::GROWING) {
    switch(txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        if (cur_lock_mode == LockMode::EXCLUSIVE || cur_lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::READ_UNCOMMITTED:
        if (cur_lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
  // modify lock queue state and txn state (remove the lock)
  for (const auto &iter : lock_queue_ptr->request_queue_) {
    if ((*iter).txn_id_ == txn->GetTransactionId()) {
      lock_queue_ptr->request_queue_.remove(iter);
      break;
    }
  }
  switch (cur_lock_mode) {
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
      break;
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].erase(rid);
      break;
    default:
      break;
  }

  // notify blocking threads
  lock_queue_ptr->cv_.notify_all();

  return true; 
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_[t1].push_back(t2);
  }
  else {
    for (auto t : waits_for_[t1]) {
      if (t == t2) {
        return;
      }
    }
    // edge does not exist
    waits_for_[t1].push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) != waits_for_.end()) {
    for (auto iter = waits_for_[t1].begin(); iter != waits_for_[t1].end(); iter++) {
      if ((*iter) == t2) {
        // remove the edge
        waits_for_[t1].erase(iter);
        return;
      }
    }
    // edge does not exist
  }
}

auto LockManager::DFSHelper(std::unordered_set<txn_id_t> &visited, txn_id_t id, txn_id_t *txn_id) -> bool {
  for (auto neigh : waits_for_[id]) {
    if (visited.find(neigh) != visited.end()) {
      txn_id_t youngest = -1;
      std::cout << "node: ";
      for (auto i : visited) {
        std::cout << i << " ";
        youngest = std::max(youngest, i);
      }
      std::cout << std::endl;
      *txn_id = youngest;
      return true;
    }
    // not visited
    visited.insert(neigh);
    if (DFSHelper(visited, neigh, txn_id)) {
      return true;
    }
    visited.erase(neigh);
  }
  return false;
}

// sort before detecting cycles
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<txn_id_t> txn_vec(0);
  for (auto &iter : waits_for_ ) {
    txn_vec.push_back(iter.first);
  }
  sort(txn_vec.begin(), txn_vec.end());

  std::unordered_set<txn_id_t> visited;
  for (auto id : txn_vec) {
    visited.insert(id);
    if (DFSHelper(visited, id, txn_id)) {
      return true;
    }
    visited.erase(id);
  }
  return false; 
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto& edge : waits_for_) {
    for (auto r_node : edge.second) {
      edges.emplace_back(edge.first, r_node);
    }
  }
  return edges;
}

template <typename Keytype>
auto LockManager::ConstructGraph(std::unordered_map<Keytype, std::shared_ptr<LockRequestQueue>> &lock_map) -> void {
  for (const auto& iter : lock_map) {
    auto request_ptr = iter.second;
    //size_t size = request_ptr->request_queue_.size();
    for (auto iter1 = request_ptr->request_queue_.begin(); iter1 != request_ptr->request_queue_.end(); iter1++) {
      if ((*iter1)->granted_) {
        continue;
      }
      for (auto iter2 = request_ptr->request_queue_.begin(); iter2 != iter1; iter2++) {
        //if (!(*iter2)->granted_) {
          AddEdge((*iter1)->txn_id_, (*iter2)->txn_id_);
        //}
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // clean and then re-construct the graph
      std::unique_lock<std::mutex> graph_lock(waits_for_latch_);
      waits_for_.clear();
      std::unique_lock<std::mutex> table_map_lock(table_lock_map_latch_);
      std::unique_lock<std::mutex> row_map_lock(row_lock_map_latch_);
      ConstructGraph(table_lock_map_);
      ConstructGraph(row_lock_map_);
      // construct a vector of txn id from low to high, also, sort the neighbors
      //txn_vec_.clear();
      for (auto &iter : waits_for_ ) {
        //txn_vec_.push_back(iter.first);
        sort(iter.second.begin(), iter.second.end());
      }
      //sort(txn_vec_.begin(), txn_vec_.end());
      // detect cycle and notify blocking threads
      txn_id_t txn_id;
      while(HasCycle(&txn_id)) {
        // remove edge, update txn status
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);

        for (auto iter = waits_for_.begin(); iter != waits_for_.end();) {
          if (iter->first == txn_id) {
            iter = waits_for_.erase(iter);
          }
          else {
            for (auto edge_iter = iter->second.begin(); edge_iter != iter->second.end();) {
              if (*edge_iter == txn_id) {
                edge_iter = iter->second.erase(edge_iter);
              } else {
                edge_iter++;
              }
            }
            iter++;
          }
        }

        // notify all threads
        for (const auto &iter : row_lock_map_) {
          iter.second->cv_.notify_all();
        }

        for (const auto &iter : table_lock_map_) {
          iter.second->cv_.notify_all();
        }
        // break;
      }

      table_map_lock.unlock();
      row_map_lock.unlock();
      graph_lock.unlock();
    }
  }
}

}  // namespace bustub
