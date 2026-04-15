下面给你一个示例，展示多个代码块，并在每个代码块前标明 `filepath:` 👇

---

**filepath: src/main/java/com/example/service/FileService.java**

```java
package com.example.service;

public class FileService {

    public void writeFile(String path, String content) {
        System.out.println("Writing to file: " + path);
        // TODO: implement write logic
    }
}
```

---

**filepath: src/main/java/com/example/controller/FileController.java**

```java
package com.example.controller;

import com.example.service.FileService;

public class FileController {

    private final FileService fileService = new FileService();

    public void createFile() {
        fileService.writeFile("test.txt", "Hello World");
    }
}
```

---

**filepath: src/main/resources/application.yml**

```yaml
server:
  port: 8080

logging:
  level:
    root: INFO
```

---

**filepath: README.md**

````md
# Example Project

This is a demo project showing how to structure files.

## Run

```bash
mvn spring-boot:run
````

```

---

如果你有特定项目（比如 Spring Boot / C 项目 / Node.js），我可以帮你**一次性生成完整工程结构代码块**。
```
