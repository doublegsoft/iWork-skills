#ifndef CFG_H
#define CFG_H

#include <tree_sitter/api.h>

typedef struct {
  char id[16];
  char type[32];
  char label[128];
} FlowNode;

typedef struct {
  char from[16];
  char to[16];
  char condition[16];
} FlowEdge;

typedef struct {
  FlowNode nodes[256];
  FlowEdge edges[512];
  int node_count;
  int edge_count;
  int id_counter;
} FlowGraph;

FlowGraph* cfg_create();
void cfg_build(FlowGraph* graph, TSNode root, const char* source);
void cfg_print_json(FlowGraph* graph);
void cfg_free(FlowGraph* graph);

#endif
