#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include "md4c.h"

/* ── file utilities ──────────────────────────────────────────── */

/**
 * Creates a file and all intermediate directories.
 * root + "/" + filepath are joined to form the full path.
 * Returns 0 on success, -1 on failure.
 */
static int create_file_with_dirs(const char* root, const char* filepath) {
  if (!root || !filepath) return -1;

  char full[1024] = {0};
  snprintf(full, sizeof(full), "%s/%s", root, filepath);

  char* path = strdup(full);
  if (!path) return -1;

  for (char* p = path + 1; *p; p++) {
    if (*p == '/') {
      *p = '\0';
      if (mkdir(path, 0755) != 0 && errno != EEXIST) {
        free(path);
        return -1;
      }
      *p = '/';
    }
  }
  free(path);

  FILE* f = fopen(full, "a");
  if (!f) return -1;
  fclose(f);
  return 0;
}

/**
 * Reads entire file into a null-terminated heap string.
 * Caller must free() the result. Returns NULL on failure.
 */
static char* read_file_as_string(const char* filepath) {
  if (!filepath) return NULL;

  FILE* f = fopen(filepath, "rb");
  if (!f) return NULL;

  fseek(f, 0, SEEK_END);
  long size = ftell(f);
  rewind(f);

  char* buf = (char*)malloc(size + 1);
  if (!buf) { fclose(f); return NULL; }

  fread(buf, 1, size, f);
  buf[size] = '\0';
  fclose(f);
  return buf;
}

/* ── demo: parse md filepath+code pairs and write files ───── */

#define _D01_MAX_FILES    64
#define _D01_MAX_PATH_LEN 512
#define _D01_MAX_CODE_LEN (1024 * 64)

typedef struct {
  char filepath[_D01_MAX_PATH_LEN];
  char code[_D01_MAX_CODE_LEN];
} D01_FileEntry;

typedef struct {
  int           in_strong;
  char          pending_path[_D01_MAX_PATH_LEN];
  int           has_pending;
  int           in_code_block;
  char          code_buf[_D01_MAX_CODE_LEN];
  int           code_len;
  D01_FileEntry entries[_D01_MAX_FILES];
  int           count;
} D01_State;

static int _d01_enter_block(MD_BLOCKTYPE type, void* detail, void* userdata) {
  D01_State* s = (D01_State*)userdata;
  if (type == MD_BLOCK_CODE) { s->in_code_block = 1; s->code_len = 0; s->code_buf[0] = '\0'; }
  (void)detail; return 0;
}

static int _d01_leave_block(MD_BLOCKTYPE type, void* detail, void* userdata) {
  D01_State* s = (D01_State*)userdata;
  if (type == MD_BLOCK_CODE) {
    s->in_code_block = 0;
    if (s->has_pending && s->count < _D01_MAX_FILES) {
      D01_FileEntry* e = &s->entries[s->count++];
      strncpy(e->filepath, s->pending_path, _D01_MAX_PATH_LEN - 1);
      strncpy(e->code,     s->code_buf,     _D01_MAX_CODE_LEN - 1);
    }
    s->has_pending = 0;
    s->pending_path[0] = '\0';
  }
  (void)detail; return 0;
}

static int _d01_enter_span(MD_SPANTYPE type, void* detail, void* userdata) {
  D01_State* s = (D01_State*)userdata;
  if (type == MD_SPAN_STRONG) s->in_strong = 1;
  (void)detail; return 0;
}

static int _d01_leave_span(MD_SPANTYPE type, void* detail, void* userdata) {
  D01_State* s = (D01_State*)userdata;
  if (type == MD_SPAN_STRONG) s->in_strong = 0;
  (void)detail; return 0;
}

static int _d01_text(MD_TEXTTYPE type, const MD_CHAR* text, MD_SIZE size, void* userdata) {
  D01_State* s = (D01_State*)userdata;
  if (s->in_strong && !s->in_code_block) {
    char buf[_D01_MAX_PATH_LEN] = {0};
    size_t len = size < sizeof(buf) - 1 ? size : sizeof(buf) - 1;
    memcpy(buf, text, len);
    const char* prefix = "filepath: ";
    size_t plen = strlen(prefix);
    if (strncmp(buf, prefix, plen) == 0) {
      strncpy(s->pending_path, buf + plen, _D01_MAX_PATH_LEN - 1);
      s->has_pending = 1;
    }
    return 0;
  }
  if (s->in_code_block && type == MD_TEXT_CODE) {
    size_t space = _D01_MAX_CODE_LEN - 1 - s->code_len;
    size_t copy  = size < space ? size : space;
    memcpy(s->code_buf + s->code_len, text, copy);
    s->code_len += copy;
    s->code_buf[s->code_len] = '\0';
  }
  return 0;
}

/**
 * Parses md_filepath, extracts filepath+code pairs,
 * writes each file under root_dir. Returns 0 on success, -1 on failure.
 */
static int process(const char* md_filepath, const char* root_dir) {
  char* content = read_file_as_string(md_filepath);
  if (!content) { fprintf(stderr, "Failed to read: %s\n", md_filepath); return -1; }

  D01_State state = {0};
  MD_PARSER parser = {
    .abi_version = 0,          .flags       = 0,
    .enter_block = _d01_enter_block, .leave_block = _d01_leave_block,
    .enter_span  = _d01_enter_span,  .leave_span  = _d01_leave_span,
    .text        = _d01_text,        .debug_log   = NULL, .syntax = NULL,
  };

  int ret = md_parse(content, (MD_SIZE)strlen(content), &parser, &state);
  free(content);
  if (ret != 0) { fprintf(stderr, "md_parse failed: %d\n", ret); return -1; }

  for (int i = 0; i < state.count; i++) {
    const char* fp   = state.entries[i].filepath;
    const char* code = state.entries[i].code;
    if (create_file_with_dirs(root_dir, fp) != 0) {
      fprintf(stderr, "Failed to create: %s/%s\n", root_dir, fp); continue;
    }
    char full[1024] = {0};
    snprintf(full, sizeof(full), "%s/%s", root_dir, fp);
    FILE* f = fopen(full, "w");
    if (!f) { fprintf(stderr, "Failed to open: %s\n", full); continue; }
    fwrite(code, 1, strlen(code), f);
    fclose(f);
    printf("wrote: %s\n", full);
  }
  return 0;
}

#endif /* COMMON_H */
