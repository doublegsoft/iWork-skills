#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "memory.h"

// ⚠️ 简化版 JSON（生产建议用 cJSON）

void memory_save(MemoryStore* store, const char* path) {
  FILE* f = fopen(path, "w");
  if (!f) return;

  fprintf(f, "{\n  \"methods\": [\n");

  for (int i = 0; i < store->count; i++) {
    MethodMemory* m = &store->methods[i];

    fprintf(f, "    {\n");
    fprintf(f, "      \"name\": \"%s\",\n", m->name);
    fprintf(f, "      \"complexity\": %d,\n", m->complexity);
    fprintf(f, "      \"level\": \"%s\",\n", m->level);

    fprintf(f, "      \"dfg\": [\n");

    for (int j = 0; j < m->dfg_count; j++) {
      fprintf(f, "        {\"from\":\"%s\",\"to\":\"%s\"}%s\n",
        m->dfg[j].from,
        m->dfg[j].to,
        j == m->dfg_count - 1 ? "" : ",");
    }

    fprintf(f, "      ]\n");
    fprintf(f, "    }%s\n", i == store->count - 1 ? "" : ",");
  }

  fprintf(f, "  ]\n}\n");

  fclose(f);
}

// 简化：不实现完整 JSON parser（后续可接 cJSON）
void memory_load(MemoryStore* store, const char* path) {
  (void)store;
  (void)path;
}
