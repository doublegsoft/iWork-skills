#ifndef DFG_H
#define DFG_H

#include <tree_sitter/api.h>

#define MAX_DFG_NODES 1024
#define MAX_DFG_EDGES 4096

typedef struct {
  char name[64];
} DFGNode;

typedef struct {
  int from;
  int to;
} DFGEdge;

typedef struct {
  DFGNode nodes[MAX_DFG_NODES];
  DFGEdge edges[MAX_DFG_EDGES];

  int node_count;
  int edge_count;
} DFG;

DFG* dfg_create();
void dfg_build(DFG* dfg, TSNode root, const char* source);
void dfg_print(DFG* dfg);
void dfg_free(DFG* dfg);

#endif
