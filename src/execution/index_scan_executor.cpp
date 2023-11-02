//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "catalog/catalog.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
//#include "pg_functions.hpp"
#include "storage/index/index_iterator.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), 
    plan_(plan),
    index_id_(plan_->GetIndexOid()), 
    index_info_(exec_ctx_->GetCatalog()->GetIndex(index_id_)),
    table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),
    b_tree_index_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *> (index_info_->index_.get())), 
    index_iterator_(b_tree_index_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    std::vector<Value> values{};
    uint32_t size = GetOutputSchema().GetColumnCount();
    values.reserve(size);
    if (index_iterator_ != b_tree_index_->GetEndIterator()) {
        *rid = (*index_iterator_).second;
        bool fetch_result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction(), true);
        ++index_iterator_;
        //LOG_INFO("fetch_result = %d, rid = %s", fetch_result, rid->ToString().data());
        return fetch_result;
    }
    return false;
}
}  // namespace bustub
