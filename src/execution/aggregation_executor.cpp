//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), 
    plan_(plan),
    child_(std::move(child)),
    aht_(plan_->aggregates_, plan_->agg_types_),
    aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
    child_->Init();
    while (true) {
        RID rid{};
        Tuple child_tuple{};
        bool status = child_->Next(&child_tuple, &rid);
        if (!status) {
            break;
        }
        AggregateKey aggr_key = MakeAggregateKey(&child_tuple);
        AggregateValue aggr_val = MakeAggregateValue(&child_tuple);
        aht_.InsertCombine(aggr_key, aggr_val);
    }
    // group by is not set => initialization, otherwise should be no output
    if (aht_.IsEmpty() && plan_->group_bys_.empty()) {
        AggregateKey aggr_key{};
        AggregateValue aggr_val{};
        aht_.InsertEmpty(aggr_key, aggr_val);
    } 
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    while (aht_iterator_ != aht_.End()) {
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        for (auto key : aht_iterator_.Key().group_bys_) {
            values.push_back(key);
        }
        for (auto val : aht_iterator_.Val().aggregates_) {
            values.push_back(val);
        }

        *tuple = Tuple(values, &GetOutputSchema());
        *rid = tuple->GetRid();
        ++aht_iterator_;
        return true;
    }

    return false; 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
