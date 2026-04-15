#ifndef IW_FUNCOMPLEX_H
#define IW_FUNCOMPLEX_H

#include <tree_sitter/api.h>

// ===== 复杂度结果 =====

typedef struct {
  int score;         // 总分
  int max_depth;     // 最大嵌套深度
  int if_count;      // if数量
  int method_chain;  // method调用链次数
} iw_complex_t;

/*!
** 分析指定节点的函数复杂度。
** 
** @param node 要分析的 TSNode（通常是 method_declaration）
** @param c 指向存储复杂度结果的结构体指针
*/
void 
iw_method_complex(TSNode node, iw_complex_t* c);

#endif // IW_FUNCOMPLEX_H
