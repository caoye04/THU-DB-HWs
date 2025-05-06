#include "include/query_engine/planner/operator/join_physical_operator.h"
#include "common/log/log.h"
#include <sstream>
using namespace std;


/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

// JoinPhysicalOperator::JoinPhysicalOperator() = default;

// // 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
// RC JoinPhysicalOperator::open(Trx *trx)
// {
//   return RC::SUCCESS;
// }

// // 计算出接下来需要输出的数据，并将结果set到join_tuple中
// // 如果没有更多数据，返回RC::RECORD_EOF
// RC JoinPhysicalOperator::next()
// {
//   return RC::RECORD_EOF;
// }

// // 节点执行完成，清理左右子算子
// RC JoinPhysicalOperator::close()
// {
//   return RC::SUCCESS;
// }

// Tuple *JoinPhysicalOperator::current_tuple()
// {
//   return &joined_tuple_;
// }

JoinPhysicalOperator::JoinPhysicalOperator() = default;

RC JoinPhysicalOperator::open(Trx *trx) 
{
  if (children_.size() < 2) {
    return RC::INTERNAL;
  }

  trx_ = trx;
  empty_result_ = false;
  is_first_next_ = true;

  // 初始化子节点
  RC status = init_child_operators(trx);
  if (status != RC::SUCCESS) {
    return status;
  }

  return RC::SUCCESS;
}

RC JoinPhysicalOperator::init_child_operators(Trx *trx)
{
  RC status = children_[0]->open(trx);
  if (status != RC::SUCCESS) {
    return status;
  }

  status = children_[1]->open(trx);
  if (status != RC::SUCCESS) {
    children_[0]->close();
  }
  
  return status;
}

RC JoinPhysicalOperator::next()
{
  if (empty_result_) {
    return RC::RECORD_EOF;
  }

  if (is_first_next_) {
    return handle_first_next();
  }

  return process_join_loop();
}

RC JoinPhysicalOperator::handle_first_next()
{
  is_first_next_ = false;
  
  RC status = fetch_initial_tuples();
  if (status != RC::SUCCESS) {
    empty_result_ = true;
    return RC::RECORD_EOF;
  }

  if (!condition_) {
    return RC::SUCCESS;
  }

  return evaluate_and_proceed();
}

RC JoinPhysicalOperator::fetch_initial_tuples()
{
  RC status = fetch_left_tuple();
  if (status != RC::SUCCESS) {
    return status;
  }

  return fetch_right_tuple();
}

RC JoinPhysicalOperator::fetch_left_tuple()
{
  RC status = children_[0]->next();
  if (status != RC::SUCCESS) {
    return status;
  }

  Tuple *left = children_[0]->current_tuple();
  if (!left) {
    return RC::INTERNAL;
  }
  
  joined_tuple_.set_left(left);
  return RC::SUCCESS;
}

RC JoinPhysicalOperator::fetch_right_tuple()
{
  RC status = children_[1]->next();
  if (status != RC::SUCCESS) {
    return status;
  }

  Tuple *right = children_[1]->current_tuple();
  if (!right) {
    return RC::INTERNAL;
  }

  joined_tuple_.set_right(right);
  return RC::SUCCESS;
}

RC JoinPhysicalOperator::process_join_loop()
{
  while (true) {
    RC status = advance_right_table();
    
    if (status == RC::SUCCESS) {
      if (check_join_condition()) {
        return RC::SUCCESS;
      }
      continue;
    }
    
    if (status == RC::RECORD_EOF) {
      status = advance_left_and_reset_right();
      if (status != RC::SUCCESS) {
        return status;
      }
      continue;
    }
    
    return status;
  }
}

RC JoinPhysicalOperator::advance_right_table()
{
  RC status = children_[1]->next();
  if (status != RC::SUCCESS) {
    return status;
  }

  Tuple *right = children_[1]->current_tuple();
  if (!right) {
    return RC::INTERNAL;
  }

  joined_tuple_.set_right(right);
  return RC::SUCCESS;
}

bool JoinPhysicalOperator::check_join_condition()
{
  if (!condition_) {
    return true;
  }

  Value value;
  RC status = condition_->get_value(joined_tuple_, value);
  return (status == RC::SUCCESS && value.get_boolean());
}

RC JoinPhysicalOperator::advance_left_and_reset_right()
{
  RC status = children_[0]->next();
  if (status != RC::SUCCESS) {
    return status;
  }

  Tuple *left = children_[0]->current_tuple();
  if (!left) {
    return RC::INTERNAL;
  }

  joined_tuple_.set_left(left);
  
  status = reset_right_table();
  if (status != RC::SUCCESS) {
    return status;
  }

  return fetch_right_tuple();
}

RC JoinPhysicalOperator::reset_right_table()
{
  RC status = children_[1]->close();
  if (status != RC::SUCCESS) {
    return status;
  }

  return children_[1]->open(trx_);
}

RC JoinPhysicalOperator::evaluate_and_proceed()
{
  Value value;
  RC status = condition_->get_value(joined_tuple_, value);
  
  if (status != RC::SUCCESS || !value.get_boolean()) {
    return process_join_loop();
  }
  
  return RC::SUCCESS;
}

RC JoinPhysicalOperator::close()
{
  RC final_status = RC::SUCCESS;

  for (auto &child : children_) {
    RC status = child->close();
    if (status != RC::SUCCESS) {
      final_status = status;
    }
  }

  empty_result_ = false;
  is_first_next_ = true;

  return final_status;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  if (empty_result_ || !validate_current_state()) {
    return nullptr;
  }

  update_joined_tuple();
  return &joined_tuple_;
}

bool JoinPhysicalOperator::validate_current_state()
{
  return children_.size() == 2 && 
         children_[0]->current_tuple() && 
         children_[1]->current_tuple();
}

void JoinPhysicalOperator::update_joined_tuple()
{
  joined_tuple_.set_left(children_[0]->current_tuple());
  joined_tuple_.set_right(children_[1]->current_tuple());
}