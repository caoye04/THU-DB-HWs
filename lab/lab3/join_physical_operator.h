#pragma once
using namespace std;

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  void set_condition(unique_ptr<Expression> condition) 
  {
    condition_ = move(condition);
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

private:
  // 辅助初始化方法
  RC init_child_operators(Trx *trx);
  
  // 处理首次next调用
  RC handle_first_next();
  RC fetch_initial_tuples();
  RC fetch_left_tuple();
  RC fetch_right_tuple();
  
  // 主要处理循环
  RC process_join_loop();
  RC advance_right_table();
  bool check_join_condition();
  RC advance_left_and_reset_right();
  RC reset_right_table();
  RC evaluate_and_proceed();
  
  // 当前元组相关方法
  bool validate_current_state();
  void update_joined_tuple();

  // 成员变量
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  unique_ptr<Expression> condition_;
  bool empty_result_ = false;
  bool is_first_next_ = true;
};