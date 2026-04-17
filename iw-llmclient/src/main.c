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
#include <argtable3.h>
#include <curl/curl.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct {
  char* data;
  size_t size;
} MemoryBuffer;

static size_t write_callback(void* contents, size_t size, size_t nmemb, void* userp) {
  size_t realsize = size * nmemb;
  MemoryBuffer* mem = (MemoryBuffer*)userp;

  char* ptr = realloc(mem->data, mem->size + realsize + 1);
  if (ptr == NULL) {
    return 0;
  }

  mem->data = ptr;
  memcpy(&(mem->data[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->data[mem->size] = 0;

  return realsize;
}

static int 
send_gemini_request(CURL* curl, const char* prompt, const char* api_key, MemoryBuffer* response) {
  // if (!api_key || strlen(api_key) == 0) {
  //   fprintf(stderr, "Error: API key is required for Gemini\n");
  //   return -1;
  // }
  
  curl_easy_reset(curl);
  
  // Build Gemini API URL with API key
  char url[4096];
  snprintf(url, sizeof(url), 
           "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=%s", 
           "AIzaSyC1GHU0eASxIjp_wfivWqVxv_f6Hv-9h9w");
  curl_easy_setopt(curl, CURLOPT_URL, url);
  
  // Simple JSON payload for Gemini
  struct curl_slist* headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  
  char post_data[4096];
  snprintf(post_data, sizeof(post_data), 
           "{\"contents\": [{\"parts\": [{\"text\": \"%s\"}]}]}", 
           prompt);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data);
  
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)response);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    fprintf(stderr, "curl failed: %s\n", curl_easy_strerror(res));
    curl_slist_free_all(headers);
    return -1;
  }

  curl_slist_free_all(headers);
  return 0;
}

static int 
send_ollama_request(CURL* curl, const char* prompt, MemoryBuffer* response) {
  curl_easy_reset(curl);
  
  // Default Ollama URL
  curl_easy_setopt(curl, CURLOPT_URL, "http://localhost:11434/api/generate");
  
  struct curl_slist* headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  // Simple JSON payload for Ollama
  char post_data[4096];
  snprintf(post_data, sizeof(post_data), 
           "{\"model\": \"gemma4:e4b\", \"prompt\": \"%s\", \"stream\": false}", 
           prompt);

  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)response);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    fprintf(stderr, "curl failed: %s\n", curl_easy_strerror(res));
    curl_slist_free_all(headers);
    return -1;
  }

  curl_slist_free_all(headers);
  return 0;
}

int main(int argc, char** argv) {
  struct arg_str*  opt_model  = arg_str0("m", "model",  "<model>",  "model name: gemini, ollama, etc.");
  struct arg_str*  opt_prompt = arg_str0("p", "prompt", "<prompt>", "the prompt to send");
  struct arg_str*  opt_apikey = arg_str0("k", "apikey", "<key>",  "API key (required for gemini)");
  struct arg_lit*  opt_help   = arg_lit0("h", "help",               "print this help and exit");
  struct arg_end*  end        = arg_end(2);

  void* argtable[] = { opt_model, opt_prompt, opt_apikey, opt_help, end };
  int nerrors = arg_parse(argc, argv, argtable);
  int exit_code = 0;
  CURL* curl = NULL;
  MemoryBuffer chunk;
  chunk.data = NULL;
  chunk.size = 0;

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

  if (opt_prompt->count == 0) {
    fprintf(stderr, "Error: -p/--prompt is required\n");
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 1;
  }

  if (opt_model->count == 0) {
    fprintf(stderr, "Error: -m/--model is required\n");
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    return 1;
  }

  const char* model = opt_model->sval[0];
  const char* prompt = opt_prompt->sval[0];
  const char* apikey = opt_apikey->count > 0 ? opt_apikey->sval[0] : NULL;

  printf("Model: %s\n", model);
  printf("Prompt: %s\n", prompt);
  printf("\n");

  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl = curl_easy_init();

  if (curl) {
    int res = 0;
    
    if (strcmp(model, "gemini") == 0) {
      res = send_gemini_request(curl, prompt, apikey, &chunk);
    } else if (strcmp(model, "ollama") == 0) {
      res = send_ollama_request(curl, prompt, &chunk);
    } else {
      fprintf(stderr, "Unsupported model: %s\n", model);
      res = -1;
    }

    if (res == 0 && chunk.data != NULL) {
      printf("Response:\n%s\n", chunk.data);
    }

    curl_easy_cleanup(curl);
  } else {
    fprintf(stderr, "Failed to initialize curl\n");
  }

  curl_global_cleanup();

  free(chunk.data);
  arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));

  return exit_code;
}
