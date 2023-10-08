//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)),
    index_id_(plan_->GetIndexOid()),    
    index_info_(exec_ctx_->GetCatalog()->GetIndex(index_id_)),    
    table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),    
    b_tree_index_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *> (index_info_->index_.get())),
    idx_(0)
    {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
    //LOG_INFO("predicate = %s", plan_->key_predicate_->ToString().data());
    //plan_->key_predicate_->Evaluate()
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    //Tuple left_tuple{};
    while (idx_ != 0 || child_executor_->Next(&left_tuple_, rid)) {
        //Tuple key = left_tuple_.KeyFromTuple(table_info_->schema_, index_info_->key_schema_, index_info_->index_->GetKeyAttrs());
        //Tuple key = left_tuple_.KeyFromTuple(child_executor_->GetOutputSchema(), );
        
        //plan_->key_predicate_->Evaluate(&left_tuple_, child_executor_->GetOutputSchema());
        
        //b_tree_index_->ScanKey(key, &results_, exec_ctx_->GetTransaction());

        std::vector<Value> key_values{};
        for (size_t i = 0; i < index_info_->key_schema_.GetColumnCount(); ++i) {
            key_values.push_back(plan_->KeyPredicate()->Evaluate(&left_tuple_, child_executor_->GetOutputSchema()));
        }
        b_tree_index_->ScanKey(Tuple(key_values, &index_info_->key_schema_), &results_, exec_ctx_->GetTransaction());

        
        //LOG_INFO("size = %ld, key = %s", results_.size(), key.GetData());
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        // left join
        if (plan_->join_type_ == JoinType::LEFT && results_.size() == 0) {
            for (uint32_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j++) {
                values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), j));
            }
            for (uint32_t j = 0; j < table_info_->schema_.GetColumnCount(); j++) { 
                values.push_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(j).GetType()));
            }
            *tuple = Tuple(values, &GetOutputSchema());
            return true;
        }
        // get right tuple
        Tuple right_tuple{};
        table_info_->table_->GetTuple(results_[idx_], &right_tuple, exec_ctx_->GetTransaction(), true);
        if (++idx_ == results_.size()) {
            idx_ = 0;
            results_.clear();
            BUSTUB_ASSERT(results_.size() == 0, "RID results vector size should be 0");
        }
        // normal inner join
        //if (true == plan_->key_predicate_->EvaluateJoin(&left_tuple_, child_executor_->GetOutputSchema(), &right_tuple, table_info_->schema_).GetAs<bool>()) {
            for (uint32_t j = 0; j < child_executor_->GetOutputSchema().GetColumnCount(); j++) {
                values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), j));
            }
            for (uint32_t j = 0; j < table_info_->schema_.GetColumnCount(); j++) {
                values.push_back(right_tuple.GetValue(&table_info_->schema_, j));
            }
            *tuple = Tuple(values, &GetOutputSchema());
            return true;
        //}
        
    }
    return false; 
}

}  // namespace bustub
