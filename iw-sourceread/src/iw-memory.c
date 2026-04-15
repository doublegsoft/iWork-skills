#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "memory.h"

// ===== 创建 =====

MemoryStore* memory_create() {
  MemoryStore* s = (MemoryStore*)malloc(sizeof(MemoryStore));
  memset(s, 0, sizeof(MemoryStore));
  return s;
}

// ===== 添加 method =====

void memory_add_method(MemoryStore* store, MethodMemory* m) {
  if (store->count >= MAX_METHODS) return;

  store->methods[store->count++] = *m;
}

// ===== 打印 =====

void memory_print(MemoryStore* store) {
  printf("=== MEMORY STORE ===\n");

  for (int i = 0; i < store->count; i++) {
    MethodMemory* m = &store->methods[i];

    printf("Method: %s\n", m->name);
    printf("Complexity: %d (%s)\n", m->complexity, m->level);

    printf("DFG:\n");
    for (int j = 0; j < m->dfg_count; j++) {
      printf("  %s -> %s\n", m->dfg[j].from, m->dfg[j].to);
    }

    printf("\n");
  }
}

// ===== 查询 =====

void memory_query_high_complexity(MemoryStore* store) {
  printf("=== HIGH COMPLEXITY METHODS ===\n");

  for (int i = 0; i < store->count; i++) {
    if (store->methods[i].complexity >= 15) {
      printf("%s (%d)\n",
        store->methods[i].name,
        store->methods[i].complexity);
    }
  }
}

// ===== 释放 =====

void memory_free(MemoryStore* store) {
  free(store);
}
