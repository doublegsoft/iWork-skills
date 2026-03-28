package com.example.controller;

import com.example.service.FileService;

public class FileController {

    private final FileService fileService = new FileService();

    public void createFile() {
        fileService.writeFile("test.txt", "Hello World");
    }
}
