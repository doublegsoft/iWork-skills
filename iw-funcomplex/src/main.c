#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tree_sitter/api.h>
#include <argtable3.h>
#include "iw-funcomplex.h"

// Java grammar
extern const TSLanguage* tree_sitter_java();

// ===== 主函数 =====

int 
main(int argc, char** argv) 
{
  struct arg_file* opt_input  = arg_file1("f", "file", "<file>", "markdown input file");
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
  char* source = NULL;

  if (!source) {
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
  
  // analyze_file(root);
  
  ts_tree_delete(tree);
  ts_parser_delete(parser);
  free(source);

  return 0;
}
