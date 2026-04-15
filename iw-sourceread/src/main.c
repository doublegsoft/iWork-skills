#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tree_sitter/api.h>
#include <argtable3.h>

#include "iw-complex.h"
#include "iw-cfg.h"

// Java grammar
extern const TSLanguage* tree_sitter_java();

// ===== 读取文件 =====

static char* read_file(const char* path) {
  FILE* fp = fopen(path, "rb");
  if (!fp) {
    printf("Error: cannot open file %s\n", path);
    return NULL;
  }

  fseek(fp, 0, SEEK_END);
  long size = ftell(fp);
  rewind(fp);

  char* buffer = (char*)malloc(size + 1);
  if (!buffer) {
    fclose(fp);
    return NULL;
  }

  fread(buffer, 1, size, fp);
  buffer[size] = '\0';

  fclose(fp);
  return buffer;
}

static void print_indent(int level) {
  for (int i = 0; i < level; i++) {
    printf("  ");
  }
}

// ===== 获取源码片段 =====

static char* get_text(TSNode node, const char* source) {
  if (ts_node_is_null(node)) {
    return NULL;
  }

  uint32_t start = ts_node_start_byte(node);
  uint32_t end = ts_node_end_byte(node);

  if (end <= start) {
    return NULL;
  }

  uint32_t len = end - start;

  char* text = (char*)malloc(len + 1);
  strncpy(text, source + start, len);
  text[len] = '\0';

  return text;
}

// ===== AST 打印 =====

static void print_ast(TSNode node, const char* source, int level) {
  if (ts_node_is_null(node)) {
    return;
  }

  const char* type = ts_node_type(node);

  print_indent(level);
  printf("%s", type);

  char* text = get_text(node, source);
  if (text && strlen(text) < 40) {
    printf(" : \"%s\"", text);
  }

  printf("\n");

  if (text) {
    free(text);
  }

  uint32_t count = ts_node_named_child_count(node);

  for (uint32_t i = 0; i < count; i++) {
    TSNode child = ts_node_named_child(node, i);
    print_ast(child, source, level + 1);
  }
}

// ===== 主函数 =====

int main(int argc, char** argv) {
  struct arg_file* opt_input  = arg_file1("f", "file", "<file>", "source code file");
  struct arg_str*  opt_out = arg_str0("o", "output",   "<ast output file>",  "output file");
  struct arg_str*  opt_lang = arg_str0("l", "language",   "<language>",  "the programming language");
  struct arg_lit*  opt_help   = arg_lit0("h", "help",               "print this help and exit");
  struct arg_end*  end        = arg_end(20);

  void* argtable[] = { opt_input, opt_out, opt_lang, opt_help, end };
  int nerrors = arg_parse(argc, argv, argtable);

  if (opt_help->count > 0) {
    printf("Usage: %s", argv[0]);
    arg_print_syntax(stdout, argtable, "\n");
    arg_print_glossary(stdout, argtable, "  %-25s %s\n");
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 0;
  }

  if (nerrors > 0) 
  {
    arg_print_errors(stderr, end, argv[0]);
    fprintf(stderr, "Try '%s --help' for more information.\n", argv[0]);
    arg_print_syntax(stdout, argtable, "\n");
    arg_print_glossary(stdout, argtable, "  %-25s %s\n");
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 1;
  }

  const char* filepath = opt_input->filename[0];
  char* source = read_file(filepath);
  if (!source) 
  {
    printf("Failed to read file.\n");
    return 1;
  }

  TSParser* parser = ts_parser_new();
  ts_parser_set_language(parser, tree_sitter_java());

  TSTree* tree = ts_parser_parse_string(
    parser,
    NULL,
    source,
    strlen(source)
  );

  TSNode root = ts_tree_root_node(tree);
  // ===== 打印 AST =====
  // print_ast(root, source, 0);
  
  // FlowGraph* graph = cfg_create();
  // cfg_build(graph, root, source);
  // cfg_print_json(graph);
  // cfg_free(graph);
  
  iw_complex_t* compls = NULL;
  int count = 0;
  iw_methods_complex(source, root, &compls, &count);

  printf("count = %d\n", count);
  for (int i = 0; i < count; i++) 
  {
    printf("%s: %d\n", compls[i].name, compls[i].score);
  }
  
  ts_tree_delete(tree);
  ts_parser_delete(parser);
  free(source);

  return 0;
}
