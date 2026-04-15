#ifndef MEMORY_H
#define MEMORY_H

#include <tree_sitter/api.h>

#define MAX_METHODS 512
#define MAX_NAME 128
#define MAX_EDGES 128

typedef struct {
  char from[MAX_NAME];
  char to[MAX_NAME];
} DFGEdge;

typedef struct {
  char name[MAX_NAME];
  int complexity;
  char level[16];

  char file[MAX_NAME];
  char module[MAX_NAME];

  DFGEdge dfg[MAX_EDGES];
  int dfg_count;

} MethodMemory;

typedef struct {
  MethodMemory methods[MAX_METHODS];
  int count;
} MemoryStore;

// ===== API =====

MemoryStore* memory_create();
void memory_add_method(MemoryStore* store, MethodMemory* m);
void memory_save(MemoryStore* store, const char* path);
void memory_load(MemoryStore* store, const char* path);

// 查询
void memory_query_high_complexity(MemoryStore* store);
void memory_print(MemoryStore* store);

void memory_free(MemoryStore* store);

#endif
