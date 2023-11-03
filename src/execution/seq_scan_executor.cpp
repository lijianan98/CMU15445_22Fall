//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstdint>
#include "catalog/catalog.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
    AbstractExecutor(exec_ctx), 
    plan_(plan), 
    table_iterator_(TableIterator(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_->Begin(exec_ctx_->GetTransaction()))) {}

void SeqScanExecutor::Init() {
    Catalog *catalog = exec_ctx_->GetCatalog();
    TableInfo *table_info = catalog->GetTable(plan_->table_oid_);
    auto txn = exec_ctx_->GetTransaction();
    // get IS table lock for RR and RC, not RUC
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        bool lock_table_res = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_info->oid_);
        if (!lock_table_res) {
            throw ExecutionException("seqscan lock table IS failed");
        }
    }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Catalog *catalog = exec_ctx_->GetCatalog();
    TableInfo *table_info = catalog->GetTable(plan_->table_oid_);
    std::vector<Value> values{};
    uint32_t size = GetOutputSchema().GetColumnCount();
    values.reserve(size);

    auto txn = exec_ctx_->GetTransaction();

    while (table_iterator_ != table_info->table_->End()) {
        // get tuple S lock for RR and RC, however for RR do not release it, for RC, can be released 
        if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
            bool lock_tuple_res = exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, table_info->oid_, table_iterator_->GetRid());
            if (!lock_tuple_res) {
                throw ExecutionException("seqscan lock tuple S failed");
            }
        }
        for (uint32_t i = 0; i < size; i++) {
            values.push_back(table_iterator_->GetValue(&GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        *rid = table_iterator_->GetRid();
        table_iterator_++;
        // unlock tuple for RC
        if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
            bool unlock_tuple_res = exec_ctx_->GetLockManager()->UnlockRow(txn, table_info->oid_, table_iterator_->GetRid());
            if (!unlock_tuple_res) {
                throw ExecutionException("seqscan unlock tuple failed");
            }
        }
        return true;
    }

    // finished seqscan all tuples, release IS lock for RC
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        bool unlock_table_res = exec_ctx_->GetLockManager()->UnlockTable(txn, table_info->oid_);
        if (!unlock_table_res) {
            throw ExecutionException("seqscan unlock table failed");
        }
    }
    return false; 
}

}  // namespace bustub
