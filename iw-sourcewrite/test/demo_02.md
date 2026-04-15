下面给你一套**完整可编译的 C 三方合并（3-way merge）最小实现**，满足你的要求：

* 每个文件独立
* 统一 2 空格缩进
* 带 filepath 标识

---

**filepath: merge.h**

```c
#ifndef MERGE_H
#define MERGE_H

char** read_lines(const char* path, int* count);

void free_lines(char** lines, int count);

void merge_files(
  char** base, int base_count,
  char** ours, int ours_count,
  char** theirs, int theirs_count
);

#endif
```

---

**filepath: merge.c**

```c
#include "merge.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE 1024

char** read_lines(const char* path, int* count) {
  FILE* fp = fopen(path, "r");
  if (!fp) {
    perror("fopen");
    return NULL;
  }

  char** lines = malloc(sizeof(char*) * 1024);
  char buffer[MAX_LINE];
  int idx = 0;

  while (fgets(buffer, MAX_LINE, fp)) {
    lines[idx] = strdup(buffer);
    idx++;
  }

  fclose(fp);
  *count = idx;
  return lines;
}

void free_lines(char** lines, int count) {
  for (int i = 0; i < count; i++) {
    free(lines[i]);
  }
  free(lines);
}

static void merge_line(
  const char* base,
  const char* ours,
  const char* theirs
) {
  if (ours && theirs && strcmp(ours, theirs) == 0) {
    printf("%s", ours);
  }
  else if (base && ours && strcmp(ours, base) == 0) {
    if (theirs) printf("%s", theirs);
  }
  else if (base && theirs && strcmp(theirs, base) == 0) {
    if (ours) printf("%s", ours);
  }
  else {
    printf("<<<<<<< ours\n");
    if (ours) printf("%s", ours);
    printf("=======\n");
    if (theirs) printf("%s", theirs);
    printf(">>>>>>> theirs\n");
  }
}

void merge_files(
  char** base, int base_count,
  char** ours, int ours_count,
  char** theirs, int theirs_count
) {
  int max = base_count;
  if (ours_count > max) max = ours_count;
  if (theirs_count > max) max = theirs_count;

  for (int i = 0; i < max; i++) {
    const char* b = (i < base_count) ? base[i] : NULL;
    const char* o = (i < ours_count) ? ours[i] : NULL;
    const char* t = (i < theirs_count) ? theirs[i] : NULL;

    merge_line(b, o, t);
  }
}
```

---

**filepath: main.c**

```c
#include "merge.h"
#include <stdio.h>

int main(int argc, char* argv[]) {

  if (argc != 4) {
    printf("Usage: %s base ours theirs\n", argv[0]);
    return 1;
  }

  int base_count = 0;
  int ours_count = 0;
  int theirs_count = 0;

  char** base = read_lines(argv[1], &base_count);
  char** ours = read_lines(argv[2], &ours_count);
  char** theirs = read_lines(argv[3], &theirs_count);

  if (!base || !ours || !theirs) {
    printf("Error reading files\n");
    return 1;
  }

  merge_files(
    base, base_count,
    ours, ours_count,
    theirs, theirs_count
  );

  free_lines(base, base_count);
  free_lines(ours, ours_count);
  free_lines(theirs, theirs_count);

  return 0;
}
```

---

**filepath: Makefile**

```makefile
CC=gcc
CFLAGS=-Wall -O2

all:
  $(CC) main.c merge.c -o merge $(CFLAGS)

clean:
  rm -f merge
```

---

# 使用方式

```bash
make
./merge base.txt ours.txt theirs.txt
```

---

# 当前能力（这个版本）

✔ 三方 merge
✔ 自动判断修改
✔ 冲突标记（和 Git 一样）

---

# 已知限制（下一步可以升级）

❗ 没有 LCS（行对齐简单）
❗ 插入/删除会错位
❗ 只是 line-level merge

---

# 如果你要升级（强烈推荐）

我可以帮你做👇

* ✔ LCS diff（接近 Git 行为）
* ✔ token-level merge（更精细）
* ✔ AST merge（C语法级）

直接说：
👉 “升级 LCS merge版本” 或 “做 Git级别merge引擎”
