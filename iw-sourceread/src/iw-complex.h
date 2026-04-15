/*
**    ▄▄▄▄  ▄▄▄  ▄▄▄▄                   
** ▀▀ ▀███  ███  ███▀            ▄▄     
** ██  ███  ███  ███ ▄███▄ ████▄ ██ ▄█▀ 
** ██  ███▄▄███▄▄███ ██ ██ ██ ▀▀ ████   
** ██▄  ▀████▀████▀  ▀███▀ ██    ██ ▀█▄ 
*/
#ifndef IW_COMPLEX_H
#define IW_COMPLEX_H

#include <tree_sitter/api.h>

// ===== 复杂度结果 =====

typedef struct {
  char name[512];    // 函数名
  int score;         // 总分
  int max_depth;     // 最大嵌套深度
  int if_count;      // if数量
  int method_chain;  // method调用链次数
} iw_complex_t;

/*!
** Calculates the complexity metrics for a method declaration.
**
** If the provided node is not a "method_declaration", this function will 
** recursively traverse its children to find one. Once found, it initializes 
** the provided `iw_complex_t` structure, extracts the method's name, delegates 
** the block analysis to calculate depth/branches, and computes a final 
** complexity score.
**
** @param source The original, full source code string.
** @param method The Tree-sitter node to analyze (or search within).
** @param c      Pointer to an `iw_complex_t` struct to store the resulting metrics.
*/
void iw_method_complex(const char* source, TSNode method, iw_complex_t* c);

/*!
** Analyzes and calculates complexity metrics for all methods within a syntax tree.
**
** If the provided `root` node is itself a "method_declaration", it evaluates 
** that single method. Otherwise, it counts all method declarations within the 
** tree, allocates an array of `iw_complex_t` structures to hold the results, 
** and populates them.
**
** @param source The original, full source code string.
** @param root   The Tree-sitter node to analyze (e.g., file root, class, or method).
** @param c      Pointer to an array of `iw_complex_t` pointers. This function 
**               will dynamically allocate the array if multiple methods are found.
** @param count  Pointer to an integer where the total number of evaluated 
**               methods will be stored.
**
** @note         If memory is allocated (i.e., *count > 0), the caller assumes 
**               ownership of `*c` and is responsible for calling free() on it.
*/
void iw_methods_complex(const char* source, TSNode root, iw_complex_t** c, int* count);

#endif // IW_COMPLEX_H
