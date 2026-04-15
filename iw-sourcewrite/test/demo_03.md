下面给你一组**可直接用于测试的 base / ours / theirs 文件数据**，专门设计了：

* ✔ 相同内容
* ✔ 单边修改
* ✔ 双边冲突
* ✔ 插入行

完全覆盖你这个 merge 程序的核心场景 👇

---

**filepath: base.txt**

```text
Line 1: Hello World
Line 2: This is a test
Line 3: Original content
Line 4: Keep this line
Line 5: End of file
```

---

**filepath: ours.txt**

```text
Line 1: Hello World
Line 2: This is a test (modified by ours)
Line 3: Original content
Line 4: Keep this line
Line 5: End of file
Line 6: Added by ours
```

---

**filepath: theirs.txt**

```text
Line 1: Hello World
Line 2: This is a test (modified by theirs)
Line 3: Original content changed by theirs
Line 4: Keep this line
Line 5: End of file
```

---

# 运行效果预期（关键点说明）

执行：

```bash
./merge base.txt ours.txt theirs.txt
```

---

## 🔍 结果分析

### Line 1

```text
Hello World
```

✔ 三方一致 → 保留

---

### Line 2（冲突）

```text
<<<<<<< ours
Line 2: This is a test (modified by ours)
=======
Line 2: This is a test (modified by theirs)
>>>>>>> theirs
```

✔ 双方修改 → 冲突

---

### Line 3（单边修改）

```text
Line 3: Original content changed by theirs
```

✔ theirs 改，ours 没改 → 用 theirs

---

### Line 6（新增）

```text
<<<<<<< ours
Line 6: Added by ours
=======
>>>>>>> theirs
```

✔ ours 新增 → 当前实现会当冲突（因为没有 LCS）

---

# 这组数据测试了什么？

| 场景   | 覆盖           |
| ---- | ------------ |
| 无修改  | ✔            |
| 单边修改 | ✔            |
| 双边冲突 | ✔            |
| 新增行  | ✔（会暴露当前算法缺陷） |

---

# 如果你要更真实（接近 Git）

我可以帮你再生成一组：

👉 **包含删除 / 插入 / 重排 的复杂测试数据**

或者直接帮你：

👉 升级你的 merge 支持 **LCS（真正像 Git 一样）**

只要说一句：
👉 “给我 LCS 测试数据” 或 “升级 merge 算法”
