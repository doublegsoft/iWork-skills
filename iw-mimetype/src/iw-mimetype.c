#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <onnxruntime_c_api.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

typedef struct {
  char* key;
  double value;
} StringDouble;

typedef struct {
  char* key;
  char* value;
} StringPair;

typedef struct {
  size_t beg_size;
  size_t mid_size;
  size_t end_size;
  bool use_inputs_at_offsets;
  double medium_confidence_threshold;
  size_t min_file_size_for_dl;
  int32_t padding_token;
  size_t block_size;
  char** target_labels_space;
  size_t target_labels_count;
  StringDouble* thresholds;
  size_t thresholds_count;
  StringPair* overwrite_map;
  size_t overwrite_count;
} ModelConfig;

typedef struct {
  char* description;
  bool is_text;
  bool found;
} ContentTypeInfo;

typedef struct {
  int32_t* data;
  size_t len;
} Features;

static const OrtApi* g_ort = NULL;

static void free_model_config(ModelConfig* config) {
  size_t i;

  if (config == NULL) {
    return;
  }
  for (i = 0; i < config->target_labels_count; ++i) {
    free(config->target_labels_space[i]);
  }
  free(config->target_labels_space);
  for (i = 0; i < config->thresholds_count; ++i) {
    free(config->thresholds[i].key);
  }
  free(config->thresholds);
  for (i = 0; i < config->overwrite_count; ++i) {
    free(config->overwrite_map[i].key);
    free(config->overwrite_map[i].value);
  }
  free(config->overwrite_map);
  memset(config, 0, sizeof(*config));
}

static void free_content_type_info(ContentTypeInfo* info) {
  if (info == NULL) {
    return;
  }
  free(info->description);
  memset(info, 0, sizeof(*info));
}

static char* read_text_file(const char* path, size_t* out_len) {
  FILE* fp;
  long size;
  char* buffer;
  size_t nread;

  fp = fopen(path, "rb");
  if (fp == NULL) {
    return NULL;
  }
  if (fseek(fp, 0, SEEK_END) != 0) {
    fclose(fp);
    return NULL;
  }
  size = ftell(fp);
  if (size < 0) {
    fclose(fp);
    return NULL;
  }
  if (fseek(fp, 0, SEEK_SET) != 0) {
    fclose(fp);
    return NULL;
  }
  buffer = malloc((size_t)size + 1);
  if (buffer == NULL) {
    fclose(fp);
    return NULL;
  }
  nread = fread(buffer, 1, (size_t)size, fp);
  fclose(fp);
  if (nread != (size_t)size) {
    free(buffer);
    return NULL;
  }
  buffer[nread] = '\0';
  if (out_len != NULL) {
    *out_len = nread;
  }
  return buffer;
}

static unsigned char* read_binary_file(const char* path, size_t* out_len) {
  size_t len = 0;
  char* text = read_text_file(path, &len);
  if (text == NULL) {
    return NULL;
  }
  if (out_len != NULL) {
    *out_len = len;
  }
  return (unsigned char*)text;
}

static const char* find_key(const char* json, const char* key) {
  char pattern[256];
  int written;

  written = snprintf(pattern, sizeof(pattern), "\"%s\":", key);
  if (written < 0 || (size_t)written >= sizeof(pattern)) {
    return NULL;
  }
  return strstr(json, pattern);
}

static const char* skip_to_value(const char* p) {
  if (p == NULL) {
    return NULL;
  }
  while (*p != '\0' && *p != ':') {
    ++p;
  }
  if (*p == ':') {
    ++p;
  }
  while (*p != '\0' && isspace((unsigned char)*p)) {
    ++p;
  }
  return p;
}

static bool parse_json_usize(const char* json, const char* key, size_t* out) {
  const char* p = skip_to_value(find_key(json, key));
  char* endptr;
  unsigned long value;

  if (p == NULL) {
    return false;
  }
  errno = 0;
  value = strtoul(p, &endptr, 10);
  if (errno != 0 || endptr == p) {
    return false;
  }
  *out = (size_t)value;
  return true;
}

static bool parse_json_int32(const char* json, const char* key, int32_t* out) {
  const char* p = skip_to_value(find_key(json, key));
  char* endptr;
  long value;

  if (p == NULL) {
    return false;
  }
  errno = 0;
  value = strtol(p, &endptr, 10);
  if (errno != 0 || endptr == p || value < INT32_MIN || value > INT32_MAX) {
    return false;
  }
  *out = (int32_t)value;
  return true;
}

static bool parse_json_double(const char* json, const char* key, double* out) {
  const char* p = skip_to_value(find_key(json, key));
  char* endptr;
  double value;

  if (p == NULL) {
    return false;
  }
  errno = 0;
  value = strtod(p, &endptr);
  if (errno != 0 || endptr == p) {
    return false;
  }
  *out = value;
  return true;
}

static bool parse_json_bool(const char* json, const char* key, bool* out) {
  const char* p = skip_to_value(find_key(json, key));

  if (p == NULL) {
    return false;
  }
  if (strncmp(p, "true", 4) == 0) {
    *out = true;
    return true;
  }
  if (strncmp(p, "false", 5) == 0) {
    *out = false;
    return true;
  }
  return false;
}

static char* parse_json_string_token(const char** cursor) {
  const char* p = *cursor;
  const char* start;
  char* out;

  while (*p != '\0' && *p != '"') {
    ++p;
  }
  if (*p != '"') {
    return NULL;
  }
  ++p;
  start = p;
  while (*p != '\0') {
    if (*p == '\\' && p[1] != '\0') {
      p += 2;
      continue;
    }
    if (*p == '"') {
      break;
    }
    ++p;
  }
  if (*p != '"') {
    return NULL;
  }
  out = malloc((p - start) + 1);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, start, (size_t)(p - start));
  out[p - start] = '\0';
  *cursor = p + 1;
  return out;
}

static bool parse_string_array(const char* json, const char* key,
                               char*** out_items, size_t* out_count) {
  const char* p = skip_to_value(find_key(json, key));
  char** items = NULL;
  size_t count = 0;

  if (p == NULL || *p != '[') {
    return false;
  }
  ++p;
  while (*p != '\0' && *p != ']') {
    char* item;
    while (*p != '\0' && (isspace((unsigned char)*p) || *p == ',')) {
      ++p;
    }
    if (*p == ']') {
      break;
    }
    item = parse_json_string_token(&p);
    if (item == NULL) {
      goto fail;
    }
    items = realloc(items, (count + 1) * sizeof(*items));
    if (items == NULL) {
      free(item);
      return false;
    }
    items[count++] = item;
    while (*p != '\0' && *p != ',' && *p != ']') {
      ++p;
    }
  }
  *out_items = items;
  *out_count = count;
  return true;

fail:
  if (items != NULL) {
    size_t i;
    for (i = 0; i < count; ++i) {
      free(items[i]);
    }
  }
  free(items);
  return false;
}

static bool parse_thresholds(const char* json, const char* key,
                             StringDouble** out_items, size_t* out_count) {
  const char* p = skip_to_value(find_key(json, key));
  StringDouble* items = NULL;
  size_t count = 0;

  if (p == NULL || *p != '{') {
    return false;
  }
  ++p;
  while (*p != '\0' && *p != '}') {
    char* name;
    char* endptr;
    double value;

    while (*p != '\0' && (isspace((unsigned char)*p) || *p == ',')) {
      ++p;
    }
    if (*p == '}') {
      break;
    }
    name = parse_json_string_token(&p);
    if (name == NULL) {
      goto fail;
    }
    while (*p != '\0' && *p != ':') {
      ++p;
    }
    if (*p != ':') {
      free(name);
      goto fail;
    }
    ++p;
    value = strtod(p, &endptr);
    if (endptr == p) {
      free(name);
      goto fail;
    }
    p = endptr;
    items = realloc(items, (count + 1) * sizeof(*items));
    if (items == NULL) {
      free(name);
      return false;
    }
    items[count].key = name;
    items[count].value = value;
    ++count;
    while (*p != '\0' && *p != ',' && *p != '}') {
      ++p;
    }
  }
  *out_items = items;
  *out_count = count;
  return true;

fail:
  if (items != NULL) {
    size_t i;
    for (i = 0; i < count; ++i) {
      free(items[i].key);
    }
  }
  free(items);
  return false;
}

static bool parse_string_map(const char* json, const char* key,
                             StringPair** out_items, size_t* out_count) {
  const char* p = skip_to_value(find_key(json, key));
  StringPair* items = NULL;
  size_t count = 0;

  if (p == NULL || *p != '{') {
    return false;
  }
  ++p;
  while (*p != '\0' && *p != '}') {
    char* map_key;
    char* map_value;

    while (*p != '\0' && (isspace((unsigned char)*p) || *p == ',')) {
      ++p;
    }
    if (*p == '}') {
      break;
    }
    map_key = parse_json_string_token(&p);
    if (map_key == NULL) {
      goto fail;
    }
    while (*p != '\0' && *p != ':') {
      ++p;
    }
    if (*p != ':') {
      free(map_key);
      goto fail;
    }
    ++p;
    map_value = parse_json_string_token(&p);
    if (map_value == NULL) {
      free(map_key);
      goto fail;
    }
    items = realloc(items, (count + 1) * sizeof(*items));
    if (items == NULL) {
      free(map_key);
      free(map_value);
      return false;
    }
    items[count].key = map_key;
    items[count].value = map_value;
    ++count;
    while (*p != '\0' && *p != ',' && *p != '}') {
      ++p;
    }
  }
  *out_items = items;
  *out_count = count;
  return true;

fail:
  if (items != NULL) {
    size_t i;
    for (i = 0; i < count; ++i) {
      free(items[i].key);
      free(items[i].value);
    }
  }
  free(items);
  return false;
}

static bool load_model_config(const char* path, ModelConfig* config) {
  char* json = read_text_file(path, NULL);
  bool ok;

  if (json == NULL) {
    return false;
  }
  memset(config, 0, sizeof(*config));
  ok = parse_json_usize(json, "beg_size", &config->beg_size) &&
       parse_json_usize(json, "mid_size", &config->mid_size) &&
       parse_json_usize(json, "end_size", &config->end_size) &&
       parse_json_bool(json, "use_inputs_at_offsets",
                       &config->use_inputs_at_offsets) &&
       parse_json_double(json, "medium_confidence_threshold",
                         &config->medium_confidence_threshold) &&
       parse_json_usize(json, "min_file_size_for_dl",
                        &config->min_file_size_for_dl) &&
       parse_json_int32(json, "padding_token", &config->padding_token) &&
       parse_json_usize(json, "block_size", &config->block_size) &&
       parse_string_array(json, "target_labels_space",
                          &config->target_labels_space,
                          &config->target_labels_count) &&
       parse_thresholds(json, "thresholds", &config->thresholds,
                        &config->thresholds_count) &&
       parse_string_map(json, "overwrite_map", &config->overwrite_map,
                        &config->overwrite_count);
  free(json);
  if (!ok) {
    free_model_config(config);
  }
  return ok;
}

static const char* lookup_overwrite(const ModelConfig* config,
                                    const char* label) {
  size_t i;
  for (i = 0; i < config->overwrite_count; ++i) {
    if (strcmp(config->overwrite_map[i].key, label) == 0) {
      return config->overwrite_map[i].value;
    }
  }
  return label;
}

static double lookup_threshold(const ModelConfig* config, const char* label) {
  size_t i;
  for (i = 0; i < config->thresholds_count; ++i) {
    if (strcmp(config->thresholds[i].key, label) == 0) {
      return config->thresholds[i].value;
    }
  }
  return config->medium_confidence_threshold;
}

static bool lookup_content_type_info(const char* kb_json, const char* label,
                                     ContentTypeInfo* info) {
  char pattern[256];
  const char* p;
  const char* is_text_pos;
  const char* desc_pos;

  memset(info, 0, sizeof(*info));
  if (snprintf(pattern, sizeof(pattern), "\"%s\":{", label) < 0) {
    return false;
  }
  p = strstr(kb_json, pattern);
  if (p == NULL) {
    return false;
  }
  desc_pos = strstr(p, "\"description\":");
  if (desc_pos != NULL) {
    desc_pos = skip_to_value(desc_pos);
    info->description = parse_json_string_token(&desc_pos);
  }
  is_text_pos = strstr(p, "\"is_text\":");
  if (is_text_pos != NULL) {
    is_text_pos = skip_to_value(is_text_pos);
    if (strncmp(is_text_pos, "true", 4) == 0) {
      info->is_text = true;
    } else if (strncmp(is_text_pos, "false", 5) == 0) {
      info->is_text = false;
    }
  }
  info->found = true;
  return true;
}

static bool is_magika_whitespace(unsigned char c) {
  return c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r' ||
         c == ' ';
}

static bool is_valid_utf8(const unsigned char* s, size_t len) {
  size_t i = 0;
  while (i < len) {
    unsigned char c = s[i];
    size_t remain;
    if (c <= 0x7F) {
      ++i;
      continue;
    }
    if ((c & 0xE0) == 0xC0) {
      remain = 1;
      if (c < 0xC2) {
        return false;
      }
    } else if ((c & 0xF0) == 0xE0) {
      remain = 2;
    } else if ((c & 0xF8) == 0xF0) {
      remain = 3;
      if (c > 0xF4) {
        return false;
      }
    } else {
      return false;
    }
    if (i + remain >= len) {
      return false;
    }
    while (remain-- > 0) {
      ++i;
      if ((s[i] & 0xC0) != 0x80) {
        return false;
      }
    }
    ++i;
  }
  return true;
}

static void copy_padded(int32_t* dst, size_t dst_size, const unsigned char* src,
                        size_t src_len, size_t prefix, int32_t padding_token) {
  size_t i;

  for (i = 0; i < dst_size; ++i) {
    dst[i] = padding_token;
  }
  for (i = 0; i < src_len && (prefix + i) < dst_size; ++i) {
    dst[prefix + i] = (int32_t)src[i];
  }
}

static Features extract_features(const unsigned char* content, size_t size,
                                 const ModelConfig* config) {
  size_t beg_block_len = size < config->block_size ? size : config->block_size;
  size_t end_block_len = beg_block_len;
  const unsigned char* beg_block = content;
  const unsigned char* mid_block;
  const unsigned char* end_block = content + size - end_block_len;
  const unsigned char* beg_trim;
  const unsigned char* end_trim_start;
  size_t beg_trim_len;
  size_t mid_start = 0;
  size_t mid_len = 0;
  size_t end_trim_len;
  Features features;
  size_t total_size = config->beg_size + config->mid_size + config->end_size;

  features.data = calloc(total_size, sizeof(*features.data));
  features.len = total_size;
  if (features.data == NULL) {
    features.len = 0;
    return features;
  }

  beg_trim = beg_block;
  while ((size_t)(beg_trim - beg_block) < beg_block_len &&
         is_magika_whitespace(*beg_trim)) {
    ++beg_trim;
  }
  beg_trim_len = beg_block_len - (size_t)(beg_trim - beg_block);
  if (beg_trim_len > config->beg_size) {
    beg_trim_len = config->beg_size;
  }

  end_trim_start = end_block;
  end_trim_len = end_block_len;
  while (end_trim_len > 0 &&
         is_magika_whitespace(end_trim_start[end_trim_len - 1])) {
    --end_trim_len;
  }
  if (end_trim_len > config->end_size) {
    end_trim_start += end_trim_len - config->end_size;
    end_trim_len = config->end_size;
  }

  if (config->mid_size > 0) {
    if (size > config->mid_size) {
      mid_start = (size - config->mid_size) / 2;
      mid_len = config->mid_size;
    } else {
      mid_start = 0;
      mid_len = size;
    }
  }
  mid_block = content + mid_start;

  copy_padded(features.data, config->beg_size, beg_trim, beg_trim_len, 0,
              config->padding_token);
  copy_padded(features.data + config->beg_size, config->mid_size, mid_block,
              mid_len,
              config->mid_size > mid_len ? (config->mid_size - mid_len) / 2 : 0,
              config->padding_token);
  copy_padded(features.data + config->beg_size + config->mid_size,
              config->end_size, end_trim_start, end_trim_len,
              config->end_size > end_trim_len ? config->end_size - end_trim_len
                                              : 0,
              config->padding_token);

  return features;
}

static void free_features(Features* features) {
  if (features == NULL) {
    return;
  }
  free(features->data);
  memset(features, 0, sizeof(*features));
}

static bool create_session(const char* model_path, OrtEnv** env,
                           OrtSession** session, OrtMemoryInfo** memory_info) {
  OrtStatus* status = NULL;
  OrtSessionOptions* options = NULL;

  *env = NULL;
  *session = NULL;
  *memory_info = NULL;

  status = g_ort->CreateEnv(ORT_LOGGING_LEVEL_ERROR, "magika-c", env);
  if (status != NULL) {
    goto fail;
  }
  status = g_ort->DisableTelemetryEvents(*env);
  if (status != NULL) {
    goto fail;
  }
  status = g_ort->CreateSessionOptions(&options);
  if (status != NULL) {
    goto fail;
  }
  status = g_ort->EnableCpuMemArena(options);
  if (status != NULL) {
    goto fail;
  }
  status = g_ort->CreateSession(*env, model_path, options, session);
  if (status != NULL) {
    goto fail;
  }
  status = g_ort->CreateCpuMemoryInfo(OrtArenaAllocator, OrtMemTypeDefault,
                                      memory_info);
  if (status != NULL) {
    goto fail;
  }
  g_ort->ReleaseSessionOptions(options);
  return true;

fail:
  if (status != NULL) {
    fprintf(stderr, "onnxruntime error: %s\n", g_ort->GetErrorMessage(status));
    g_ort->ReleaseStatus(status);
  }
  if (options != NULL) {
    g_ort->ReleaseSessionOptions(options);
  }
  if (*memory_info != NULL) {
    g_ort->ReleaseMemoryInfo(*memory_info);
    *memory_info = NULL;
  }
  if (*session != NULL) {
    g_ort->ReleaseSession(*session);
    *session = NULL;
  }
  if (*env != NULL) {
    g_ort->ReleaseEnv(*env);
    *env = NULL;
  }
  return false;
}

static bool run_inference(OrtSession* session, OrtMemoryInfo* memory_info,
                          const Features* features, size_t target_count,
                          float** out_scores) {
  const char* input_names[] = {"bytes"};
  const char* output_names[] = {"target_label"};
  int64_t input_shape[2];
  OrtValue* input_tensor = NULL;
  OrtValue* output_tensor = NULL;
  OrtStatus* status = NULL;
  float* scores = NULL;
  float* raw_scores = NULL;

  input_shape[0] = 1;
  input_shape[1] = (int64_t)features->len;
  scores = calloc(target_count, sizeof(*scores));
  if (scores == NULL) {
    return false;
  }

  status = g_ort->CreateTensorWithDataAsOrtValue(
    memory_info, (void*)features->data, features->len * sizeof(*features->data),
    input_shape, 2, ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32, &input_tensor);
  if (status != NULL) {
    goto fail;
  }
  status = g_ort->Run(session, NULL, input_names,
                      (const OrtValue* const*)&input_tensor, 1, output_names, 1,
                      &output_tensor);
  if (status != NULL) {
    goto fail;
  }
  status = g_ort->GetTensorMutableData(output_tensor, (void**)&raw_scores);
  if (status != NULL) {
    goto fail;
  }
  memcpy(scores, raw_scores, target_count * sizeof(*scores));
  g_ort->ReleaseValue(input_tensor);
  g_ort->ReleaseValue(output_tensor);
  *out_scores = scores;
  return true;

fail:
  if (status != NULL) {
    fprintf(stderr, "onnxruntime error: %s\n", g_ort->GetErrorMessage(status));
    g_ort->ReleaseStatus(status);
  }
  if (input_tensor != NULL) {
    g_ort->ReleaseValue(input_tensor);
  }
  if (output_tensor != NULL) {
    g_ort->ReleaseValue(output_tensor);
  }
  free(scores);
  return false;
}

static const char* fallback_label_for_bytes(const unsigned char* content,
                                            size_t size) {
  if (is_valid_utf8(content, size)) {
    return "txt";
  }
  return "unknown";
}

static void join_path(char* buffer, size_t buffer_size, const char* dir,
                      const char* file) {
  size_t dir_len = strlen(dir);
  if (dir_len > 0 && dir[dir_len - 1] == '/') {
    snprintf(buffer, buffer_size, "%s%s", dir, file);
  } else {
    snprintf(buffer, buffer_size, "%s/%s", dir, file);
  }
}

int main(int argc, char** argv) {
  const char* model_dir = "../assets/models/standard_v3_3";
  const char* input_path = NULL;
  char config_path[PATH_MAX];
  char model_path[PATH_MAX];
  char kb_path[PATH_MAX];
  ModelConfig config;
  ContentTypeInfo info;
  char* kb_json = NULL;
  unsigned char* content = NULL;
  size_t content_len = 0;
  const char* final_label;
  const char* dl_label = "undefined";
  double score = 1.0;
  Features features;
  OrtEnv* env = NULL;
  OrtSession* session = NULL;
  OrtMemoryInfo* memory_info = NULL;
  int exit_code = 1;
  int i;

  memset(&config, 0, sizeof(config));
  memset(&info, 0, sizeof(info));
  memset(&features, 0, sizeof(features));

  for (i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--model-dir") == 0) {
      if (i + 1 >= argc) {
        fprintf(stderr, "--model-dir requires a value\n");
        goto cleanup;
      }
      model_dir = argv[++i];
    } else if (argv[i][0] == '-') {
      fprintf(stderr, "unsupported option: %s\n", argv[i]);
      goto cleanup;
    } else {
      input_path = argv[i];
    }
  }

  if (input_path == NULL) {
    fprintf(stderr, "usage: %s [--model-dir DIR] FILE\n", argv[0]);
    goto cleanup;
  }

  join_path(config_path, sizeof(config_path), model_dir, "config.min.json");
  join_path(model_path, sizeof(model_path), model_dir, "model.onnx");
  join_path(kb_path, sizeof(kb_path), "../python/src/magika/config",
            "content_types_kb.min.json");

  if (!load_model_config(config_path, &config)) {
    fprintf(stderr, "failed to load model config: %s\n", config_path);
    goto cleanup;
  }
  if (config.use_inputs_at_offsets) {
    fprintf(
      stderr,
      "models using inputs_at_offsets are not supported by this prototype\n");
    goto cleanup;
  }
  kb_json = read_text_file(kb_path, NULL);
  if (kb_json == NULL) {
    fprintf(stderr, "failed to load content types KB: %s\n", kb_path);
    goto cleanup;
  }
  content = read_binary_file(input_path, &content_len);
  if (content == NULL) {
    fprintf(stderr, "failed to read input file: %s\n", input_path);
    goto cleanup;
  }

  if (content_len == 0) {
    final_label = "empty";
    goto print_result;
  }

  features = extract_features(content, content_len, &config);
  if (features.data == NULL) {
    fprintf(stderr, "failed to extract features\n");
    goto cleanup;
  }

  if (content_len < config.min_file_size_for_dl ||
      features.data[config.min_file_size_for_dl - 1] == config.padding_token) {
    final_label = fallback_label_for_bytes(content, content_len);
    goto print_result;
  }

  g_ort = OrtGetApiBase()->GetApi(ORT_API_VERSION);
  if (g_ort == NULL) {
    fprintf(stderr, "failed to initialize onnxruntime api\n");
    goto cleanup;
  }
  if (!create_session(model_path, &env, &session, &memory_info)) {
    goto cleanup;
  }

  {
    float* scores = NULL;
    size_t best = 0;
    size_t idx;
    const char* overwritten;
    double threshold;

    if (!run_inference(session, memory_info, &features,
                       config.target_labels_count, &scores)) {
      goto cleanup;
    }
    for (idx = 1; idx < config.target_labels_count; ++idx) {
      if (scores[idx] > scores[best]) {
        best = idx;
      }
    }
    dl_label = config.target_labels_space[best];
    score = scores[best];
    overwritten = lookup_overwrite(&config, dl_label);
    threshold = lookup_threshold(&config, dl_label);
    final_label = overwritten;

    if (score < threshold) {
      ContentTypeInfo output_info;
      if (lookup_content_type_info(kb_json, overwritten, &output_info) &&
          output_info.is_text) {
        final_label = "txt";
      } else {
        final_label = "unknown";
      }
      free_content_type_info(&output_info);
    }
    free(scores);
  }

print_result:
  lookup_content_type_info(kb_json, final_label, &info);
  printf("path: %s\n", input_path);
  printf("output_label: %s\n", final_label);
  printf("dl_label: %s\n", dl_label);
  printf("score: %.6f\n", score);
  if (info.description != NULL) {
    printf("description: %s\n", info.description);
  }
  exit_code = 0;

cleanup:
  free_content_type_info(&info);
  free_features(&features);
  free(content);
  free(kb_json);
  free_model_config(&config);
  if (memory_info != NULL && g_ort != NULL) {
    g_ort->ReleaseMemoryInfo(memory_info);
  }
  if (session != NULL && g_ort != NULL) {
    g_ort->ReleaseSession(session);
  }
  if (env != NULL && g_ort != NULL) {
    g_ort->ReleaseEnv(env);
  }
  return exit_code;
}
