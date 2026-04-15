cmake_minimum_required(VERSION 3.10)
project(iw-html2image)

set(CMAKE_C_STANDARD 11)

include_directories(include)

add_executable(${PROJECT_NAME} src/main.c)
