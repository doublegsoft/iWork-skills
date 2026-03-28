cmake_minimum_required(VERSION 3.16)
project(MyProject VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# --- argtable3 via FetchContent ---
include(FetchContent)
FetchContent_Declare(
  argtable3
  GIT_REPOSITORY https://github.com/argtable/argtable3.git
  GIT_TAG        master
)
FetchContent_MakeAvailable(argtable3)

include_directories(src)

add_executable(${PROJECT_NAME}
    src/main.c
    src/merge.c
)

target_link_libraries(${PROJECT_NAME} PRIVATE argtable3)
