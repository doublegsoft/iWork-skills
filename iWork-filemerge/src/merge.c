#include "merge.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE 1024

/* ------------------------------------------------------------------ */
/* I/O helpers                                                          */
/* ------------------------------------------------------------------ */

char** read_lines(const char* path, int* count) {
  FILE* fp = fopen(path, "r");
  if (!fp) { perror("fopen"); return NULL; }
  int cap = 64;
  char** lines = malloc(sizeof(char*) * cap);
  char buf[MAX_LINE];
  int idx = 0;
  while (fgets(buf, MAX_LINE, fp)) {
    if (idx == cap) { cap *= 2; lines = realloc(lines, sizeof(char*) * cap); }
    lines[idx++] = strdup(buf);
  }
  fclose(fp);
  *count = idx;
  return lines;
}

void free_lines(char** lines, int count) {
  for (int i = 0; i < count; i++) free(lines[i]);
  free(lines);
}

/* ------------------------------------------------------------------ */
/* Edit script                                                          */
/* ------------------------------------------------------------------ */

typedef enum { OP_EQ, OP_INS, OP_DEL } OpType;
typedef struct { OpType type; int a_idx; int b_idx; } Edit;
typedef struct { Edit* edits; int count; int cap; } EditList;

static void el_push(EditList* el, OpType t, int ai, int bi) {
  if (el->count == el->cap) {
    el->cap = el->cap ? el->cap * 2 : 64;
    el->edits = realloc(el->edits, sizeof(Edit) * el->cap);
  }
  el->edits[el->count++] = (Edit){ t, ai, bi };
}

/* ------------------------------------------------------------------ */
/* Myers diff                                                           */
/* ------------------------------------------------------------------ */

static void myers_diff(char** A, int n, char** B, int m, EditList* el) {
  int max = n + m;
  if (max == 0) return;
  int* v = calloc(2 * max + 1, sizeof(int));
  int** trace = malloc((max + 1) * sizeof(int*));
  int d, found_d = 0, found_k = 0;
  for (d = 0; d <= max; d++) {
    trace[d] = malloc((2 * max + 1) * sizeof(int));
    memcpy(trace[d], v, (2 * max + 1) * sizeof(int));
    for (int k = -d; k <= d; k += 2) {
      int x;
      if (k == -d || (k != d && v[k-1+max] < v[k+1+max]))
        x = v[k+1+max];
      else
        x = v[k-1+max] + 1;
      int y = x - k;
      while (x < n && y < m && strcmp(A[x], B[y]) == 0) { x++; y++; }
      v[k+max] = x;
      if (x >= n && y >= m) { found_d = d; found_k = k; goto done; }
    }
  }
done:;
  Edit* rev = malloc((n + m + 1) * sizeof(Edit));
  int rev_cnt = 0;
  int x = n, y = m, k = found_k;
  for (d = found_d; d > 0; d--) {
    int* pv = trace[d-1];
    int prev_k = (k == -d || (k != d && pv[k-1+max] < pv[k+1+max])) ? k+1 : k-1;
    int prev_x = pv[prev_k+max];
    int prev_y = prev_x - prev_k;
    while (x > prev_x && y > prev_y) { x--; y--; rev[rev_cnt++] = (Edit){OP_EQ,x,y}; }
    if (x > prev_x) { x--; rev[rev_cnt++] = (Edit){OP_DEL,x,y}; }
    else             { y--; rev[rev_cnt++] = (Edit){OP_INS,x,y}; }
    k = prev_k;
  }
  while (x > 0 && y > 0) { x--; y--; rev[rev_cnt++] = (Edit){OP_EQ,x,y}; }
  for (int i = rev_cnt-1; i >= 0; i--) el_push(el, rev[i].type, rev[i].a_idx, rev[i].b_idx);
  free(rev); free(v);
  for (int i = 0; i <= found_d; i++) free(trace[i]);
  free(trace);
}

/* ------------------------------------------------------------------ */
/* Hunk extraction                                                      */
/* ------------------------------------------------------------------ */

typedef struct { int base_start, base_end, new_start, new_end; } Hunk;
typedef struct { Hunk* hunks; int count, cap; } HunkList;

static void hl_push(HunkList* hl, Hunk h) {
  if (hl->count == hl->cap) {
    hl->cap = hl->cap ? hl->cap * 2 : 16;
    hl->hunks = realloc(hl->hunks, sizeof(Hunk) * hl->cap);
  }
  hl->hunks[hl->count++] = h;
}

static HunkList edits_to_hunks(EditList* el) {
  HunkList hl = {0};
  int i = 0;
  while (i < el->count) {
    if (el->edits[i].type == OP_EQ) { i++; continue; }
    int bs = el->edits[i].a_idx, ns = el->edits[i].b_idx;
    int be = bs, ne = ns;
    while (i < el->count && el->edits[i].type != OP_EQ) {
      Edit* e = &el->edits[i];
      if (e->type == OP_DEL) be = e->a_idx + 1;
      else                   ne = e->b_idx + 1;
      i++;
    }
    hl_push(&hl, (Hunk){bs, be, ns, ne});
  }
  return hl;
}

static int hunks_overlap(Hunk* a, Hunk* b) {
  return a->base_start < b->base_end && b->base_start < a->base_end;
}

static int lines_equal(char** A, int as, int ae, char** B, int bs, int be) {
  if (ae - as != be - bs) return 0;
  for (int i = 0; i < ae - as; i++)
    if (strcmp(A[as+i], B[bs+i]) != 0) return 0;
  return 1;
}

static void print_lines(char** lines, int start, int end) {
  for (int i = start; i < end; i++) printf("%s", lines[i]);
}

/* ------------------------------------------------------------------ */
/* 2-way diff                                                           */
/* ------------------------------------------------------------------ */

static void diff2(char** base, int bn, char** ours, int on) {
  EditList el = {0};
  myers_diff(base, bn, ours, on, &el);
  HunkList hl = edits_to_hunks(&el);
  int base_pos = 0;
  for (int i = 0; i < hl.count; i++) {
    Hunk* h = &hl.hunks[i];
    print_lines(base, base_pos, h->base_start);
    printf("<<<<<<< base\n");
    print_lines(base, h->base_start, h->base_end);
    printf("=======\n");
    print_lines(ours, h->new_start, h->new_end);
    printf(">>>>>>> ours\n");
    base_pos = h->base_end;
  }
  print_lines(base, base_pos, bn);
  free(hl.hunks); free(el.edits);
}

/* ------------------------------------------------------------------ */
/* 3-way merge                                                          */
/* Returns 0 if clean merge, 1 if conflicts found                      */
/* ------------------------------------------------------------------ */

int merge_files(
  char** base, int bn,
  char** ours, int on,
  char** theirs, int tn
) {
  if (!theirs) { diff2(base, bn, ours, on); return 0; }

  EditList el_o = {0}, el_t = {0};
  myers_diff(base, bn, ours,   on, &el_o);
  myers_diff(base, bn, theirs, tn, &el_t);

  HunkList hl_o = edits_to_hunks(&el_o);
  HunkList hl_t = edits_to_hunks(&el_t);
  free(el_o.edits); free(el_t.edits);

  int base_pos = 0, oi = 0, ti = 0, has_conflict = 0;

  while (oi < hl_o.count || ti < hl_t.count) {
    Hunk* ho = oi < hl_o.count ? &hl_o.hunks[oi] : NULL;
    Hunk* ht = ti < hl_t.count ? &hl_t.hunks[ti] : NULL;
    Hunk* first = (!ht || (ho && ho->base_start <= ht->base_start)) ? ho : ht;

    print_lines(base, base_pos, first->base_start);
    base_pos = first->base_start;

    if (ho && ht && hunks_overlap(ho, ht)) {
      int be = ho->base_end > ht->base_end ? ho->base_end : ht->base_end;
      if (lines_equal(ours, ho->new_start, ho->new_end,
                      theirs, ht->new_start, ht->new_end)) {
        /* identical change on both sides: auto-merge */
        print_lines(ours, ho->new_start, ho->new_end);
      } else {
        /* real conflict */
        has_conflict = 1;
        printf("<<<<<<< ours\n");
        print_lines(ours,   ho->new_start, ho->new_end);
        printf("=======\n");
        print_lines(theirs, ht->new_start, ht->new_end);
        printf(">>>>>>> theirs\n");
      }
      base_pos = be;
      oi++; ti++;
    } else if (first == ho) {
      /* only ours changed this region */
      print_lines(ours, ho->new_start, ho->new_end);
      base_pos = ho->base_end;
      oi++;
    } else {
      /* only theirs changed this region */
      print_lines(theirs, ht->new_start, ht->new_end);
      base_pos = ht->base_end;
      ti++;
    }
  }
  /* emit remaining unchanged lines */
  print_lines(base, base_pos, bn);

  free(hl_o.hunks); free(hl_t.hunks);
  return has_conflict;
}
