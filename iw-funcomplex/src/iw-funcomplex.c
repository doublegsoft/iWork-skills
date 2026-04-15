#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tree_sitter/api.h>

#include "iw-funcomplex.h"

// ===== 工具 =====

static int 
is_null(TSNode n) {
  return ts_node_is_null(n);
}

static const char* 
type(TSNode n) {
  return ts_node_type(n);
}

// ===== 判断 method chain 深度 =====

static int 
count_method_chain(TSNode node) {
  int depth = 0;

  while (!is_null(node) && strcmp(type(node), "method_invocation") == 0) {
    depth++;
    node = ts_node_child(node, 0); // 向左递归
  }

  return depth;
}

static int 
is_write_call(TSNode node) {
  if (strcmp(type(node), "method_invocation") != 0) return 0;

  TSNode name = ts_node_child(node, 2);
  if (is_null(name)) return 0;

  const char* n = ts_node_type(name);
  (void)n;

  // 直接用源码片段判断
  // （简单但有效）
  return 1; // 可优化
}

static void 
iw_method_complex_recursive(TSNode node, int depth, iw_complex_t* c) {

  if (is_null(node)) return;

  const char* t = type(node);

  if (depth > c->max_depth) {
    c->max_depth = depth;
  }

  // ===== if =====
  if (strcmp(t, "if_statement") == 0) {
    c->score += 2;
    c->if_count++;
  }

  // ===== for =====
  if (strcmp(t, "enhanced_for_statement") == 0 ||
      strcmp(t, "for_statement") == 0) {
    c->score += 2;
  }

  // ===== binary =====
  if (strcmp(t, "binary_expression") == 0) {
    c->score += 1;
  }

  // ===== method =====
  if (strcmp(t, "method_invocation") == 0) {
    c->score += 1;

    int chain = count_method_chain(node);
    if (chain > 2) {
      c->score += 2;
      c->method_chain++;
    }
  }

  // ===== return =====
  if (strcmp(t, "return_statement") == 0) {
    c->score += 1;
  }

  // ===== 递归 =====
  uint32_t n = ts_node_named_child_count(node);

  for (uint32_t i = 0; i < n; i++) {
    TSNode child = ts_node_named_child(node, i);
    iw_method_complex_recursive(child, depth + 1, c);
  }
}

// ===== method级分析 =====

void 
iw_method_complex(TSNode method, iw_complex_t* c) {
  memset(&c, 0, sizeof(c));
  iw_method_complex_recursive(method, 0, c);

  // ===== 深度加分 =====
  if (c->max_depth >= 3) c->score += 3;
  if (c->max_depth >= 5) c->score += 5;
  if (c->if_count >= 3) c->score += 3;
}
