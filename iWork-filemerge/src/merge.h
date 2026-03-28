#ifndef MERGE_H
#define MERGE_H

char** read_lines(const char* path, int* count);
void   free_lines(char** lines, int count);

/* Returns 0 = clean merge, 1 = conflicts found */
int merge_files(
  char** base,   int base_count,
  char** ours,   int ours_count,
  char** theirs, int theirs_count  /* NULL for 2-way diff */
);

#endif
