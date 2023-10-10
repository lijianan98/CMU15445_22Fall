#include <memory>
#include "catalog/schema.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  //LOG_INFO("%s\n", plan->ToString().data());
  //for (const auto &c : plan->children_) {
    //LOG_INFO("%s\n", c->ToString().data());
  //}
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit &&
  optimized_plan->children_[0]->GetType() == PlanType::Sort) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*optimized_plan->children_[0]);
    return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, optimized_plan->GetChildAt(0), 
      sort_plan.GetOrderBy(), limit_plan.limit_);
  }

  return optimized_plan;
}

}  // namespace bustub
