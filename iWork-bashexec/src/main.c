#include <stdio.h>
#include <argtable3.h>

int main(int argc, char *argv[])
{
  struct arg_lit *help = arg_lit0("h", "help", "print this help and exit");
  struct arg_end *end  = arg_end(20);
  void *argtable[]     = { help, end };

  int exitcode = 0;

  if (arg_parse(argc, argv, argtable) > 0) {
    arg_print_errors(stderr, end, argv[0]);
    exitcode = 1;
    goto exit;
  }

  if (help->count > 0) {
    printf("Usage: %s", argv[0]);
    arg_print_syntax(stdout, argtable, "\n");
    arg_print_glossary(stdout, argtable, "  %-25s %s\n");
    goto exit;
  }

  printf("Hello, World!\n");

exit:
  arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
  return exitcode;
}
