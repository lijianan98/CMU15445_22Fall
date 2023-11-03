//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/logger.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), 
    plan_(plan),
    child_executor_(std::move(child_executor)), 
    finished_(false) {}

void DeleteExecutor::Init() { 
    child_executor_->Init();

    Catalog *catalog = exec_ctx_->GetCatalog();
    TableInfo *table_info = catalog->GetTable(plan_->table_oid_);
    auto txn = exec_ctx_->GetTransaction();
    // get IX table lock for all isolation levels
    bool lock_table_res = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info->oid_);
    if (!lock_table_res) {
        throw ExecutionException("delete lock table IX failed");
    }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
        // the rid of the tuple to delete is stored here
        const auto status = child_executor_->Next(&child_tuple, rid);
        if (!status) {
            finished_ = true;
            break;
        }

        // add tuple lock, however, no need to update txn write set here, as InsertTuple already handles this part
        bool lock_tuple_res = exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info->oid_, *rid);
        if (!lock_tuple_res) {
            throw ExecutionException("delete lock tuple X fail");
        }

        //LOG_INFO("to_delete_rid = %s, child_tuple = %s, rid = %s", rid->ToString().data(), child_tuple.ToString(&table_info->schema_).data(), rid->ToString().data());
        bool delete_result = table_info->table_->MarkDelete(*rid, txn);
        if (!delete_result) {   // if tuple does not exist
            continue;   
        }
        for (auto index_info : index_vec) {
            Tuple key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
            index_info->index_->DeleteEntry(key, *rid, txn);
        }
        //LOG_INFO("delete from indexes");
        cnt++;
    }
    //LOG_INFO("cnt = %d", cnt);
    std::vector<Value> values{};
    values.push_back(ValueFactory::GetIntegerValue(cnt));
    *tuple = Tuple{values, &GetOutputSchema()};

    return true; 
}

}  // namespace bustub
