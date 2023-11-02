#include "execution/executors/topn_executor.h"
#include <queue>
#include <utility>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)) {
        child_executor_->Init();
        Tuple child_tuple{};
        RID rid{};
        auto compare_func = [&](std::pair<Tuple, RID> &pair1, std::pair<Tuple, RID> &pair2) -> bool {
            for (const auto & order_by : plan_->order_bys_) {
                const auto &left = order_by.second->Evaluate(&pair1.first, child_executor_->GetOutputSchema());
                const auto &right = order_by.second->Evaluate(&pair2.first, child_executor_->GetOutputSchema());
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
        //LOG_INFO("TopN is called");
        std::priority_queue<std::pair<Tuple, RID>, std::vector<std::pair<Tuple, RID>>, decltype(compare_func)> pq(compare_func);
        while (child_executor_->Next(&child_tuple, &rid)) {
            //LOG_INFO("one tuple got");
            pq.emplace(child_tuple, rid);
            if (pq.size() > plan_->n_) {
                pq.pop();
            }
        }
        while (!pq.empty()) {
            results_.push(pq.top());
            pq.pop();
        }
        //LOG_INFO("size = %ld", results_.size());
        idx_ = 0;
    }

void TopNExecutor::Init() {}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (!results_.empty()) {
        *tuple = results_.top().first;
        *rid = results_.top().second;
        results_.pop();
        return true;
    }
    return false; 
}

}  // namespace bustub
