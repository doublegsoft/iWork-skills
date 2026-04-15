#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "iw-dfg.h"

// ===== 工具 =====

static int find_or_add_node(DFG* dfg, const char* name) {
  for (int i = 0; i < dfg->node_count; i++) {
    if (strcmp(dfg->nodes[i].name, name) == 0) {
      return i;
    }
  }

  int id = dfg->node_count++;
  strcpy(dfg->nodes[id].name, name);
  return id;
}

static void add_edge(DFG* dfg, const char* from, const char* to) {
  int f = find_or_add_node(dfg, from);
  int t = find_or_add_node(dfg, to);

  dfg->edges[dfg->edge_count].from = f;
  dfg->edges[dfg->edge_count].to = t;
  dfg->edge_count++;
}

// ===== 提取 identifier =====

static void collect_identifiers(TSNode node, const char* source, char ids[][64], int* count) {
  if (ts_node_is_null(node)) return;

  const char* type = ts_node_type(node);

  if (strcmp(type, "identifier") == 0) {
    uint32_t s = ts_node_start_byte(node);
    uint32_t e = ts_node_end_byte(node);

    int len = e - s;
    strncpy(ids[*count], source + s, len);
    ids[*count][len] = '\0';

    (*count)++;
    return;
  }

  uint32_t n = ts_node_named_child_count(node);

  for (uint32_t i = 0; i < n; i++) {
    collect_identifiers(ts_node_named_child(node, i), source, ids, count);
  }
}

// ===== 处理赋值 =====

static void handle_assignment(DFG* dfg, TSNode node, const char* source) {
  TSNode left = ts_node_child(node, 0);
  TSNode right = ts_node_child(node, 2);

  char lhs[64] = {0};

  // 获取左值
  if (strcmp(ts_node_type(left), "identifier") == 0) {
    uint32_t s = ts_node_start_byte(left);
    uint32_t e = ts_node_end_byte(left);
    strncpy(lhs, source + s, e - s);
    lhs[e - s] = '\0';
  }

  // 收集右侧变量
  char ids[32][64];
  int count = 0;

  collect_identifiers(right, source, ids, &count);

  for (int i = 0; i < count; i++) {
    add_edge(dfg, ids[i], lhs);
  }
}

// ===== 处理变量声明 =====

static void handle_declaration(DFG* dfg, TSNode node, const char* source) {
  uint32_t n = ts_node_named_child_count(node);

  for (uint32_t i = 0; i < n; i++) {
    TSNode child = ts_node_named_child(node, i);

    if (strcmp(ts_node_type(child), "variable_declarator") == 0) {
      TSNode name = ts_node_child_by_field_name(child, "name", 4);
      TSNode value = ts_node_child_by_field_name(child, "value", 5);

      if (ts_node_is_null(name) || ts_node_is_null(value)) continue;

      char lhs[64];
      uint32_t s = ts_node_start_byte(name);
      uint32_t e = ts_node_end_byte(name);
      strncpy(lhs, source + s, e - s);
      lhs[e - s] = '\0';

      char ids[32][64];
      int count = 0;

      collect_identifiers(value, source, ids, &count);

      for (int j = 0; j < count; j++) {
        add_edge(dfg, ids[j], lhs);
      }
    }
  }
}

// ===== return =====

static void handle_return(DFG* dfg, TSNode node, const char* source) {
  char ids[32][64];
  int count = 0;

  collect_identifiers(node, source, ids, &count);

  for (int i = 0; i < count; i++) {
    add_edge(dfg, ids[i], "return");
  }
}

// ===== method call（参数依赖）=====

static void handle_method_call(DFG* dfg, TSNode node, const char* source) {
  char ids[32][64];
  int count = 0;

  collect_identifiers(node, source, ids, &count);

  for (int i = 0; i < count; i++) {
    add_edge(dfg, ids[i], "call");
  }
}

// ===== 遍历 =====

static void traverse(DFG* dfg, TSNode node, const char* source) {
  if (ts_node_is_null(node)) return;

  const char* type = ts_node_type(node);

  if (strcmp(type, "assignment_expression") == 0) {
    handle_assignment(dfg, node, source);
  }

  if (strcmp(type, "local_variable_declaration") == 0) {
    handle_declaration(dfg, node, source);
  }

  if (strcmp(type, "return_statement") == 0) {
    handle_return(dfg, node, source);
  }

  if (strcmp(type, "method_invocation") == 0) {
    handle_method_call(dfg, node, source);
  }

  uint32_t n = ts_node_named_child_count(node);

  for (uint32_t i = 0; i < n; i++) {
    traverse(dfg, ts_node_named_child(node, i), source);
  }
}

// ===== API =====

DFG* dfg_create() {
  DFG* d = (DFG*)malloc(sizeof(DFG));
  memset(d, 0, sizeof(DFG));
  return d;
}

void dfg_build(DFG* dfg, TSNode root, const char* source) {
  traverse(dfg, root, source);
}

void dfg_print(DFG* dfg) {
  printf("=== DFG ===\n");

  for (int i = 0; i < dfg->edge_count; i++) {
    printf("%s -> %s\n",
      dfg->nodes[dfg->edges[i].from].name,
      dfg->nodes[dfg->edges[i].to].name);
  }
}

void dfg_free(DFG* dfg) {
  free(dfg);
}
