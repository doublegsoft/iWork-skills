#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "iw-cfg.h"

typedef struct {
  int last;
  int terminated;
} FlowState;

// ===== 基础 =====

static int add_node(FlowGraph* g, const char* type, const char* label) {
  int id = g->node_count;
  sprintf(g->nodes[id].id, "n%d", g->id_counter++);
  strcpy(g->nodes[id].type, type);
  strcpy(g->nodes[id].label, label ? label : "");
  g->node_count++;
  return id;
}

static void add_edge(FlowGraph* g, int from, int to, const char* cond) {
  strcpy(g->edges[g->edge_count].from, g->nodes[from].id);
  strcpy(g->edges[g->edge_count].to, g->nodes[to].id);
  strcpy(g->edges[g->edge_count].condition, cond ? cond : "");
  g->edge_count++;
}

static char* get_text(TSNode n, const char* src) {
  if (ts_node_is_null(n)) return NULL;

  uint32_t s = ts_node_start_byte(n);
  uint32_t e = ts_node_end_byte(n);
  if (e <= s) return NULL;

  char* t = (char*)malloc(e - s + 1);
  strncpy(t, src + s, e - s);
  t[e - s] = 0;
  return t;
}

// ===== 核心 =====

static FlowState process_stmt(FlowGraph* g, TSNode node, const char* src, FlowState in, int end);

// return
static FlowState handle_return(FlowGraph* g, TSNode node, const char* src, FlowState in, int end) {
  char* txt = get_text(node, src);

  int id = add_node(g, "return", txt ? txt : "return");
  add_edge(g, in.last, id, NULL);
  add_edge(g, id, end, NULL);

  if (txt) free(txt);

  FlowState out = { id, 1 }; // terminated
  return out;
}

// block
static FlowState handle_block(FlowGraph* g, TSNode node, const char* src, FlowState in, int end) {
  FlowState cur = in;

  uint32_t n = ts_node_named_child_count(node);
  for (uint32_t i = 0; i < n; i++) {
    TSNode c = ts_node_named_child(node, i);

    if (cur.terminated) break;

    cur = process_stmt(g, c, src, cur, end);
  }

  return cur;
}

// if（关键）
static FlowState handle_if(FlowGraph* g, TSNode node, const char* src, FlowState in, int end) {
  TSNode cond = ts_node_child_by_field_name(node, "condition", 9);
  TSNode cons = ts_node_child_by_field_name(node, "consequence", 11);
  TSNode alt  = ts_node_child_by_field_name(node, "alternative", 11);

  char* txt = get_text(cond, src);

  int cond_id = add_node(g, "if", txt ? txt : "if");
  add_edge(g, in.last, cond_id, NULL);

  if (txt) free(txt);

  // then
  FlowState then_state = { cond_id, 0 };
  if (!ts_node_is_null(cons)) {
    then_state = process_stmt(g, cons, src, then_state, end);
  }

  // else
  FlowState else_state = { cond_id, 0 };
  if (!ts_node_is_null(alt)) {
    else_state = process_stmt(g, alt, src, else_state, end);
  }

  // merge
  FlowState out;

  if (then_state.terminated && else_state.terminated) {
    out.terminated = 1;
    out.last = then_state.last;
  } else {
    int merge = add_node(g, "merge", "merge");

    if (!then_state.terminated) {
      add_edge(g, then_state.last, merge, NULL);
    }

    if (!else_state.terminated) {
      add_edge(g, else_state.last, merge, NULL);
    }

    out.last = merge;
    out.terminated = 0;
  }

  return out;
}

// dispatcher
static FlowState process_stmt(FlowGraph* g, TSNode node, const char* src, FlowState in, int end) {
  if (ts_node_is_null(node)) return in;

  const char* t = ts_node_type(node);

  if (strcmp(t, "return_statement") == 0) {
    return handle_return(g, node, src, in, end);
  }

  if (strcmp(t, "if_statement") == 0) {
    return handle_if(g, node, src, in, end);
  }

  if (strcmp(t, "block") == 0) {
    return handle_block(g, node, src, in, end);
  }

  // fallback
  uint32_t n = ts_node_named_child_count(node);
  FlowState cur = in;

  for (uint32_t i = 0; i < n; i++) {
    TSNode c = ts_node_named_child(node, i);
    cur = process_stmt(g, c, src, cur, end);
  }

  return cur;
}

// ===== 入口 =====

void cfg_build(FlowGraph* g, TSNode root, const char* src) {
  int start = add_node(g, "start", "start");
  int end   = add_node(g, "end", "end");

  uint32_t n = ts_node_named_child_count(root);

  for (uint32_t i = 0; i < n; i++) {
    TSNode m = ts_node_named_child(root, i);

    if (strcmp(ts_node_type(m), "method_declaration") == 0) {
      TSNode body = ts_node_child_by_field_name(m, "body", 4);

      FlowState init = { start, 0 };
      FlowState out  = process_stmt(g, body, src, init, end);

      if (!out.terminated) {
        add_edge(g, out.last, end, NULL);
      }
    }
  }
}
