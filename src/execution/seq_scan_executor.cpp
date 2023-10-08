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
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
    AbstractExecutor(exec_ctx), 
    plan_(plan), 
    table_iterator_(TableIterator(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_->Begin(exec_ctx_->GetTransaction()))) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Catalog *catalog = exec_ctx_->GetCatalog();
    TableInfo *table_info = catalog->GetTable(plan_->table_oid_);
    std::vector<Value> values{};
    uint32_t size = GetOutputSchema().GetColumnCount();
    values.reserve(size);
    while (table_iterator_ != table_info->table_->End()) {
        for (uint32_t i = 0; i < size; i++) {
            values.push_back(table_iterator_->GetValue(&GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        *rid = table_iterator_->GetRid();
        table_iterator_++;
        return true;
    }
    return false; 
}

}  // namespace bustub
