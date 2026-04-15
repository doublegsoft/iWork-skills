#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef __APPLE__
#define CHROME_PATH                                                            \
  "\"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome\""
#else
#define CHROME_PATH "google-chrome"
#endif

void print_usage(const char *prog_name) {
  printf("Usage: %s <input_html> <output_image>\n", prog_name);
}

int main(int argc, char *argv[]) {
  if (argc != 3) {
    print_usage(argv[0]);
    return 1;
  }

  const char *input_html = argv[1];
  const char *output_image = argv[2];

  char abs_input_path[PATH_MAX];
  if (realpath(input_html, abs_input_path) == NULL) {
    perror("realpath");
    return 1;
  }

  char command[PATH_MAX * 2 + 256];
  snprintf(command, sizeof(command),
           "%s --headless --disable-gpu --screenshot=\"%s\" \"file://%s\"",
           CHROME_PATH, output_image, abs_input_path);

  printf("Executing command: %s\n", command);

  int ret = system(command);
  if (ret == -1) {
    perror("system");
    return 1;
  }

  if (WIFEXITED(ret) && WEXITSTATUS(ret) == 0) {
    printf("Successfully converted %s to %s\n", input_html, output_image);
    return 0;
  } else {
    fprintf(stderr, "Chrome command failed with exit code %d\n",
            WEXITSTATUS(ret));
    return 1;
  }
}
