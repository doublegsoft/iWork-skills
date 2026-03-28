#include "merge.h"
#include <argtable3.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char* argv[]) {
  struct arg_file* arg_base   = arg_file1(NULL, NULL, "<base>",   "base file");
  struct arg_file* arg_ours   = arg_file1(NULL, NULL, "<ours>",   "ours file");
  struct arg_file* arg_theirs = arg_file0(NULL, NULL, "<theirs>", "theirs file (optional, omit for 2-way diff)");
  struct arg_lit*  arg_help   = arg_lit0("h", "help", "print this help and exit");
  struct arg_end*  end        = arg_end(20);

  void* argtable[] = { arg_base, arg_ours, arg_theirs, arg_help, end };

  int nerrors = arg_parse(argc, argv, argtable);

  if (arg_help->count > 0) {
    printf("Usage: %s", argv[0]);
    arg_print_syntax(stdout, argtable, "\n");
    arg_print_glossary(stdout, argtable, "  %-25s %s\n");
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 0;
  }

  if (nerrors > 0) {
    arg_print_errors(stderr, end, argv[0]);
    fprintf(stderr, "Usage: %s", argv[0]);
    arg_print_syntax(stderr, argtable, "\n");
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 1;
  }

  int base_count = 0, ours_count = 0, theirs_count = 0;

  char** base   = read_lines(arg_base->filename[0],   &base_count);
  char** ours   = read_lines(arg_ours->filename[0],   &ours_count);
  char** theirs = (arg_theirs->count > 0)
                    ? read_lines(arg_theirs->filename[0], &theirs_count)
                    : NULL;

  if (!base || !ours || (arg_theirs->count > 0 && !theirs)) {
    fprintf(stderr, "Error reading input files\n");
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 1;
  }

  int has_conflict = merge_files(
    base,   base_count,
    ours,   ours_count,
    theirs, theirs_count
  );

  free_lines(base, base_count);
  free_lines(ours, ours_count);
  if (theirs) free_lines(theirs, theirs_count);

  arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));

  /* exit 0 = clean merge, 1 = conflicts (same convention as git merge-file) */
  return has_conflict ? 1 : 0;
}
