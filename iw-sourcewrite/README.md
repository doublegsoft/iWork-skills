iWork-sourcewrite
=================

## 提示词

```
要求：
1. 在每个code block前面生成filepath: 文字，指明生成的文件路径和名称
2. left-aligned style, 代码缩进为2
3。 给我完整code block，单独是一个文件
```

## 命令执行

**iWork-sourcewrite**

```
iWork-sourcewrite -m <md文件> -o <输出路径>
```

**iWork-sourceread**

```
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/Users/christian/export/local/works/doublegsoft.ai/iWork/03.Development/iWork-skills/3rd/tree-sitter:/Users/christian/export/local/works/doublegsoft.ai/iWork/03.Development/iWork-skills/3rd/tree-sitter/test/fixtures/grammars/java

iWork-sourceread -m <md文件> -o <输出路径>
```