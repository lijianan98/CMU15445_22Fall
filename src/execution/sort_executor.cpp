#include "execution/executors/sort_executor.h"
#include <cstdint>
#include "binder/bound_order_by.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), 
    plan_(plan),
    child_executor_(std::move(child_executor)) {
        //LOG_INFO("size = %ld", child_tuples_.size());
        Tuple child_tuple{};
        RID rid{};
        while (child_executor_->Next(&child_tuple, &rid)) {
            child_tuples_.emplace_back(child_tuple, rid);
        }
        //LOG_INFO("size = %ld", child_tuples_.size());
        auto compare_func = [&](std::pair<Tuple, RID> &pair1, std::pair<Tuple, RID> &pair2) -> bool {
            for (const auto & order_by : plan_->order_bys_) {
                auto left = order_by.second->Evaluate(&pair1.first, child_executor_->GetOutputSchema());
                auto right = order_by.second->Evaluate(&pair2.first, child_executor_->GetOutputSchema());
                if (left.CompareEquals(right) == CmpBool::CmpTrue) {
                    continue;
                }
                if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT) {
                    return (left.CompareLessThan(right) == CmpBool::CmpTrue);
                }
                // now DESC
                return (left.CompareGreaterThan(right) == CmpBool::CmpTrue);
            }
            return true;
        };
        std::sort(child_tuples_.begin(), child_tuples_.end(), compare_func);
        idx_ = 0;
    }

void SortExecutor::Init() {}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (child_tuples_.empty() || idx_ == child_tuples_.size()) {
        return false;
    }
    *tuple = child_tuples_[idx_].first;
    *rid = child_tuples_[idx_].second;
    idx_++;
    return true;
}

}  // namespace bustub
