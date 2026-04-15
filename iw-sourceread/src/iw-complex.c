/*
**    ▄▄▄▄  ▄▄▄  ▄▄▄▄                   
** ▀▀ ▀███  ███  ███▀            ▄▄     
** ██  ███  ███  ███ ▄███▄ ████▄ ██ ▄█▀ 
** ██  ███▄▄███▄▄███ ██ ██ ██ ▀▀ ████   
** ██▄  ▀████▀████▀  ▀███▀ ██    ██ ▀█▄ 
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tree_sitter/api.h>

#include "iw-complex.h"

// ===== 工具 =====

static int is_null(TSNode n) {
  return ts_node_is_null(n);
}

static const char* type(TSNode n) {
  return ts_node_type(n);
}

/*!
** Extracts the exact text of a given Tree-sitter node from the source code.
**
** @param source The original, full source code string.
** @param node   The Tree-sitter node whose text needs to be extracted.
**
** @return       A newly heap-allocated, null-terminated string containing the 
**               node's text. Returns NULL if the node is invalid, empty, or 
**               if memory allocation fails.
**
** @note         The caller assumes ownership of the returned string and is 
**               responsible for calling free() on it.
*/
static char* 
iw_node_text(const char* source, TSNode node) {
  if (is_null(node)) return NULL;
  
  uint32_t start = ts_node_start_byte(node);
  uint32_t end = ts_node_end_byte(node);
  
  if (end <= start) return NULL;
  
  uint32_t len = end - start;
  char* text = (char*)malloc(len + 1);
  if (!text) return NULL;
  
  strncpy(text, source + start, len);
  text[len] = '\0';
  
  return text;
}

/*!
** Extracts the identifier (name) of a method from a method_declaration node.
**
** @param source      The original, full source code string.
** @param method_decl The Tree-sitter node representing a method declaration.
**
** @return            A newly heap-allocated, null-terminated string containing 
**                    the method's name. Returns NULL if the node is invalid, 
**                    not a method declaration, or lacks an identifier.
**
** @note              The caller assumes ownership of the returned string and 
**                    is responsible for calling free() on it.
*/
static char* 
iw_method_name(const char* source, TSNode method_decl) 
{
  if (is_null(method_decl) || 
      strcmp(type(method_decl), "method_declaration") != 0)
    return NULL;
  
  uint32_t n = ts_node_named_child_count(method_decl);
  
  for (uint32_t i = 0; i < n; i++) 
  {
    TSNode child = ts_node_named_child(method_decl, i);
    if (strcmp(type(child), "identifier") == 0)
      return iw_node_text(source, child);
  }

  return NULL;
}

/*!
** Calculates the depth of a method invocation chain.
**
** This function measures how deeply methods are chained together 
** (e.g., `obj.method1().method2().method3()`). It recursively traverses 
** the left-most child of a "method_invocation" node to count the total 
** number of chained calls.
**
** @param node The Tree-sitter node representing the starting point of the 
**             method invocation chain.
**
** @return     The total depth (number of chained method calls). Returns 0 
**             if the initial node is not a "method_invocation" or is null.
*/
static int 
iw_method_chain(TSNode node) 
{
  int depth = 0;
  while (!is_null(node) && strcmp(type(node), "method_invocation") == 0) {
    depth++;
    node = ts_node_child(node, 0); // 向左递归
  }
  return depth;
}

/*!
** Evaluates the complexity of a method block.
**
** This function recursively traverses the syntax tree of a method's body, 
** incrementing a complexity score based on various structural elements like 
** conditions (`if_statement`), loops (`for_statement`), logical expressions 
** (`binary_expression`), method invocations, and return statements. It also 
** tracks the maximum nesting depth and the occurrence of method chains.
**
** @param node   The Tree-sitter node currently being analyzed.
** @param depth  The current nesting depth within the method block.
** @param c      A pointer to an `iw_complex_t` structure that accumulates 
**               the calculated complexity metrics.
*/
static void 
iw_method_block_complex(TSNode node, int depth, iw_complex_t* c) {
  if (is_null(node)) return;

  const char* t = type(node);

  // ===== 深度 =====
  if (depth > c->max_depth) {
    c->max_depth = depth;
  }

  // ===== if =====
  if (strcmp(t, "if_statement") == 0) {
    c->score += 2;
    c->if_count++;
  }

  if (strcmp(t, "enhanced_for_statement") == 0 ||
      strcmp(t, "for_statement") == 0) {
    c->score += 2;
  }

  // ===== binary =====
  if (strcmp(t, "binary_expression") == 0) {
    c->score += 1;
  }

  // ===== method =====
  if (strcmp(t, "method_invocation") == 0) {
    c->score += 1;

    int chain = iw_method_chain(node);
    if (chain > 2) {
      c->score += 2;
      c->method_chain++;
    }
  }

  // ===== return =====
  if (strcmp(t, "return_statement") == 0) {
    c->score += 1;
  }

  // ===== 递归 =====
  uint32_t n = ts_node_named_child_count(node);

  for (uint32_t i = 0; i < n; i++) {
    TSNode child = ts_node_named_child(node, i);
    iw_method_block_complex(child, depth + 1, c);
  }
}

/*!
** Recursively counts the total number of method declarations within a given 
** Tree-sitter node and its descendants.
**
** This function traverses the syntax tree, examining each named child node. 
** If a node is identified as a "method_declaration", it increments the count. 
** Otherwise, it recursively calls itself on the child node to continue searching.
**
** @param parent The starting Tree-sitter node (e.g., a file root or class body) 
**               to search for method declarations.
** @param count  A pointer to an integer where the total number of found 
**               "method_declaration" nodes will be accumulated. This value 
**               is updated incrementally during the traversal.
*/
static void 
iw_methods_count(TSNode node, int* count) {
  if (ts_node_is_null(node)) return;

  uint32_t n = ts_node_named_child_count(node);
  for (uint32_t i = 0; i < n; i++) 
  {
    TSNode child = ts_node_named_child(node, i);
    const char* t = type(child);
    if (strcmp(t, "method_declaration") == 0)
      (*count)++;
    else
      iw_methods_count(child, count);
  }
}

static void 
iw_method_complex_internal(const char* source, 
                           TSNode method, 
                           iw_complex_t** all,
                           int* index) 
{
  const char* t = type(method);
  if (strcmp(t, "method_declaration") != 0) 
  {
    uint32_t n = ts_node_named_child_count(method);
    for (int i = 0; i < n; i++)
    {
      TSNode child = ts_node_named_child(method, i);
      iw_method_complex_internal(source, child, all, index);
    }
    return;
  }
  
  iw_method_complex(source, method, &(*all)[*index]);
  (*index)++;
}

void 
iw_method_complex(const char* source, TSNode method, iw_complex_t* c) 
{
  const char* t = type(method);
  if (strcmp(t, "method_declaration") != 0) 
  {
    uint32_t n = ts_node_named_child_count(method);
    for (int i = 0; i < n; i++)
    {
      TSNode child = ts_node_named_child(method, i);
      iw_method_complex(source, child, c);
    }
    return;
  }
  
  memset(c, 0, sizeof(iw_complex_t));
  
  char* name = iw_method_name(source, method);
  if (name) {
    strncpy(c->name, name, sizeof(c->name) - 1);
    c->name[sizeof(c->name) - 1] = '\0';
    free(name);
  }
  
  iw_method_block_complex(method, 0, c);

  if (c->max_depth >= 3) c->score += 3;
  if (c->max_depth >= 5) c->score += 5;

  if (c->if_count >= 3) c->score += 3;
  if (c->if_count >= 5) c->score += 5;
}

void 
iw_methods_complex(const char* source, TSNode root, iw_complex_t** c, int* count) {
  if (ts_node_is_null(root)) return;

  const char* t = type(root);
  if (strcmp(t, "method_declaration") == 0) {
    iw_method_complex(source, root, *c);
    return;
  }
  *count = 0;
  iw_methods_count(root, count);
  
  if (*count > 0) {
    *c = malloc(sizeof(iw_complex_t) * (*count));
    memset(*c, 0, sizeof(iw_complex_t) * (*count));
  }
  int index = 0;
  iw_method_complex_internal(source, root, c, &index);
}
