//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <math.h>
#include <algorithm>
#include <cstdint>
#include <memory>

#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor): 
    AbstractExecutor(exec_ctx), 
    plan_(plan),
    child_executor_(std::move(child_executor)),
    finished_(false) {}

void InsertExecutor::Init() {
    child_executor_->Init();
    //throw NotImplementedException("InsertExecutor is not implemented"); 
    Catalog *catalog = exec_ctx_->GetCatalog();
    TableInfo *table_info = catalog->GetTable(plan_->table_oid_);
    auto txn = exec_ctx_->GetTransaction();
    // get IX table lock for all isolation levels
    bool lock_table_res = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info->oid_);
    if (!lock_table_res) {
        throw ExecutionException("insert lock table IX failed");
    }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    if (finished_) {
        return false;
    }
    int32_t cnt = 0;
    Catalog *catalog = exec_ctx_->GetCatalog();
    TableInfo *table_info = catalog->GetTable(plan_->table_oid_);
    auto index_vec = catalog->GetTableIndexes(table_info->name_);
    Transaction *txn = exec_ctx_->GetTransaction();
    // Get the next tuple
    while (true) {
        Tuple child_tuple{};
        const auto status = child_executor_->Next(&child_tuple, rid);
        if (!status) {
            finished_ = true;
            break;
        }
        //LOG_INFO("fetch one tuple");
        // store inserted tuple's rid here, then update all indexes
        table_info->table_->InsertTuple(child_tuple, rid, txn);
        
        // add tuple lock, however, no need to update txn write set here, as InsertTuple already handles this part
        bool lock_tuple_res = exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info->oid_, *rid);
        if (!lock_tuple_res) {
            throw ExecutionException("insert lock tuple X fail");
        }


        for (auto index_info : index_vec) {
            Tuple key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
            index_info->index_->InsertEntry(key, *rid, txn);
        }
        cnt++;
    }
    //LOG_INFO("cnt = %d", cnt);
    std::vector<Value> values{};
    values.push_back(ValueFactory::GetIntegerValue(cnt));
    *tuple = Tuple{values, &GetOutputSchema()};
    return true; 
}

}  // namespace bustub
