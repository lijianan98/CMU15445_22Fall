//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_executor)),
    right_executor_(std::move(right_executor))
    {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    while (true) {
        Tuple tuple{};
        RID rid{};
        if (!right_executor_->Next(&tuple, &rid)) {
            break;
        }
        right_tuples_.push_back(tuple);
    }
    r_idx_ = -1;
    found_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while (r_idx_ != (uint32_t)-1 || left_executor_->Next(&left_tuple_, rid)) {
    //while (r_idx_ != right_tuples_.size() || left_executor_->Next(&left_tuple_, rid)) {
        // same part for both inner join and left join
        if (r_idx_ == (uint32_t)-1) {
        //if (r_idx_ == right_tuples_.size()) {
            r_idx_ = 0;
            found_matched_ = false;
        }

        for (; r_idx_ < right_tuples_.size(); r_idx_++) {
            if (!plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuples_[r_idx_], right_executor_->GetOutputSchema()).GetAs<bool>()) {
                continue; 
            }
            //LOG_INFO("\nfound matched tuple pairs:\n%s\n%s", left_tuple_.ToString(&(left_executor_->GetOutputSchema())).data(), right_tuples_[r_idx_].ToString(&(right_executor_->GetOutputSchema())).data());
            std::vector<Value> values{};
            values.reserve(GetOutputSchema().GetColumnCount());
            for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) {
                values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
            }
            for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
                values.push_back(right_tuples_[r_idx_].GetValue(&right_executor_->GetOutputSchema(), j));
            }
            // move to next index
            r_idx_++;
            found_matched_ = true;
            *tuple = Tuple(values, &GetOutputSchema());
            return true;
        }
        // set r_idx_ to -1 if has finished processing all right tuples
        if (r_idx_ == right_tuples_.size()) {
            r_idx_ = -1; 
        }

        // didn't find any match pair in current call to Next and previous ones
        // left join
        if (plan_->GetJoinType() == JoinType::LEFT && !found_matched_) {
            //LOG_INFO("left join, no matched pairs found");
            std::vector<Value> values{};
            values.reserve(GetOutputSchema().GetColumnCount());
            for (uint32_t j = 0; j < left_executor_->GetOutputSchema().GetColumnCount(); j++) { 
                values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), j));
            }
            for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
                values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(j).GetType()));
            }
            *tuple = Tuple(values, &GetOutputSchema());
            return true;
        }
    }
    return false; 
}

}  // namespace bustub
