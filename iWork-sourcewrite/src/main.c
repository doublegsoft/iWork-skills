#include <stdio.h>
#include <string.h>
#include <argtable3.h>
#include "common.h"

static int indent_level = 0;

static void print_indent() {
  for (int i = 0; i < indent_level; i++) {
    printf("  ");
  }
}

static const char* block_type_name(MD_BLOCKTYPE type) {
  switch (type) {
    case MD_BLOCK_DOC:   return "DOCUMENT";
    case MD_BLOCK_QUOTE: return "QUOTE";
    case MD_BLOCK_UL:    return "UNORDERED_LIST";
    case MD_BLOCK_OL:    return "ORDERED_LIST";
    case MD_BLOCK_LI:    return "LIST_ITEM";
    case MD_BLOCK_HR:    return "HORIZONTAL_RULE";
    case MD_BLOCK_H:     return "HEADING";
    case MD_BLOCK_CODE:  return "CODE_BLOCK";
    case MD_BLOCK_HTML:  return "HTML_BLOCK";
    case MD_BLOCK_P:     return "PARAGRAPH";
    case MD_BLOCK_TABLE: return "TABLE";
    case MD_BLOCK_THEAD: return "TABLE_HEAD";
    case MD_BLOCK_TBODY: return "TABLE_BODY";
    case MD_BLOCK_TR:    return "TABLE_ROW";
    case MD_BLOCK_TH:    return "TABLE_HEADER_CELL";
    case MD_BLOCK_TD:    return "TABLE_DATA_CELL";
    default:             return "UNKNOWN";
  }
}

static const char* span_type_name(MD_SPANTYPE type) {
  switch (type) {
    case MD_SPAN_EM:                return "EMPHASIS";
    case MD_SPAN_STRONG:            return "STRONG";
    case MD_SPAN_A:                 return "LINK";
    case MD_SPAN_IMG:               return "IMAGE";
    case MD_SPAN_CODE:              return "CODE";
    case MD_SPAN_DEL:               return "STRIKETHROUGH";
    case MD_SPAN_LATEXMATH:         return "LATEX_MATH";
    case MD_SPAN_LATEXMATH_DISPLAY: return "LATEX_MATH_DISPLAY";
    case MD_SPAN_WIKILINK:          return "WIKILINK";
    case MD_SPAN_U:                 return "UNDERLINE";
    default:                        return "UNKNOWN";
  }
}

static int enter_block(MD_BLOCKTYPE type, void* detail, void* userdata) {
  (void)userdata;
  print_indent();
  printf("ENTER %s", block_type_name(type));
  if (type == MD_BLOCK_H) {
    MD_BLOCK_H_DETAIL* h = (MD_BLOCK_H_DETAIL*)detail;
    printf(" (level=%u)", h->level);
  } else if (type == MD_BLOCK_OL) {
    MD_BLOCK_OL_DETAIL* ol = (MD_BLOCK_OL_DETAIL*)detail;
    printf(" (start=%u)", ol->start);
  } else if (type == MD_BLOCK_CODE) {
    MD_BLOCK_CODE_DETAIL* code = (MD_BLOCK_CODE_DETAIL*)detail;
    if (code->lang.text) {
      printf(" (lang=");
      fwrite(code->lang.text, 1, code->lang.size, stdout);
      printf(")");
    }
  }
  printf("\n");
  indent_level++;
  return 0;
}

static int leave_block(MD_BLOCKTYPE type, void* detail, void* userdata) {
  (void)detail; (void)userdata;
  indent_level--;
  print_indent();
  printf("LEAVE %s\n", block_type_name(type));
  return 0;
}

static int enter_span(MD_SPANTYPE type, void* detail, void* userdata) {
  (void)userdata;
  print_indent();
  printf("ENTER %s", span_type_name(type));
  if (type == MD_SPAN_A) {
    MD_SPAN_A_DETAIL* a = (MD_SPAN_A_DETAIL*)detail;
    printf(" (href=");
    fwrite(a->href.text, 1, a->href.size, stdout);
    printf(")");
  } else if (type == MD_SPAN_IMG) {
    MD_SPAN_IMG_DETAIL* img = (MD_SPAN_IMG_DETAIL*)detail;
    printf(" (src=");
    fwrite(img->src.text, 1, img->src.size, stdout);
    printf(")");
  }
  printf("\n");
  indent_level++;
  return 0;
}

static int leave_span(MD_SPANTYPE type, void* detail, void* userdata) {
  (void)detail; (void)userdata;
  indent_level--;
  print_indent();
  printf("LEAVE %s\n", span_type_name(type));
  return 0;
}

static int text_cb(MD_TEXTTYPE type, const MD_CHAR* text, MD_SIZE size, void* userdata) {
  (void)type; (void)userdata;
  print_indent();
  printf("TEXT: \"");
  fwrite(text, 1, size, stdout);
  printf("\"\n");
  return 0;
}

int main(int argc, char* argv[]) {
  struct arg_file* opt_input  = arg_file1("m", "markdown", "<file>", "markdown input file");
  struct arg_str*  opt_output = arg_str0("o", "output",   "<dir>",  "output root directory (default: out)");
  struct arg_lit*  opt_help   = arg_lit0("h", "help",               "print this help and exit");
  struct arg_end*  end        = arg_end(20);

  void* argtable[] = { opt_input, opt_output, opt_help, end };

  int nerrors = arg_parse(argc, argv, argtable);

  if (opt_help->count > 0) {
    printf("Usage: %s", argv[0]);
    arg_print_syntax(stdout, argtable, "\n");
    arg_print_glossary(stdout, argtable, "  %-25s %s\n");
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 0;
  }

  if (nerrors > 0) {
    arg_print_errors(stderr, end, argv[0]);
    fprintf(stderr, "Try '%s --help' for more information.\n", argv[0]);
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 1;
  }

  const char* md_file  = opt_input->filename[0];
  const char* root_dir = opt_output->count > 0 ? opt_output->sval[0] : "out";

  int ret = process(md_file, root_dir);
  arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
  return ret == 0 ? 0 : 1;
}
