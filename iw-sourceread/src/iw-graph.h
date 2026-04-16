/*
**    ▄▄▄▄  ▄▄▄  ▄▄▄▄                   
** ▀▀ ▀███  ███  ███▀            ▄▄     
** ██  ███  ███  ███ ▄███▄ ████▄ ██ ▄█▀ 
** ██  ███▄▄███▄▄███ ██ ██ ██ ▀▀ ████   
** ██▄  ▀████▀████▀  ▀███▀ ██    ██ ▀█▄ 
*/
#ifndef IW_GRAPH_H
#define IW_GRAPH_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct 
{
  char id[16];
  char type[32];
  char label[128];
} iw_graph_node_t;

typedef struct 
{
  char from[16];
  char to[16];
  char condition[16];
} iw_graph_edge_t;

typedef struct 
{
  iw_graph_node_t nodes[256];
  iw_graph_edge_t edges[512];
  int node_count;
  int edge_count;
  int id_counter;
} iw_graph_t;

#ifdef __cplusplus
}
#endif

#endif // IW_GRAPH_H
