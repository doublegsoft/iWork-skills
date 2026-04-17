iWork-skills
============

## 基本协议

### 代码格式要求

**请求格式**：

```
我要求的代码格式：

1. 缩进为2
2. 如果是C语言和C++语言，pointer Left-Aligned (Type-Oriented)
3. 返回的代码，在代码块之前描述清楚文件路径，格式为filepath: <文件路径>

你记住了吗？那我们就正式开始。
```

### 返回代码要求

**请求格式**：

```
我要求的代码的返回格式：

filepath: <文件路径>

<完整文件代码>
```

### 项目相关内容请求

**请求格式**：

```
<code block>或者<output block>

描述诉求，请求帮助。
```

### 项目结构

## 工具

### iw-codemerge

代码合并，从文件到文件的替换，文件中局部方法的合并与替换。

### iw-html2image

html文件转换成图片。

### iw-funcomplex

函数复杂度计算。


```bash

iw-funcomplexity -c <完整代码>
```

### iw-funflow

函数的逻辑流程，包括Control Flow Graph和Data Flow Graph。

### iw-sourcewrite

解析markdown协议，解析出满足一下协议格式的内容就执行。协议如下：

```
filepath: <文件路径>

<完整文件代码>
```

### iw-probuild

项目编译，支持maven、cargo、cmake等等。

## 运行环境 

```
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/Users/christian/export/local/works/doublegsoft.ai/iWork/03.Development/iWork-skills/3rd/tree-sitter:/Users/christian/export/local/works/doublegsoft.ai/iWork/03.Development/iWork-skills/3rd/tree-sitter/test/fixtures/grammars/java
```