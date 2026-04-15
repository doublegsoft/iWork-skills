cmake_minimum_required(VERSION 3.16)
project(iw-sourceread C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED True)

if (WIN32)
elseif (APPLE)
  set(PLATFORM                              "darwin")
elseif (UNIX)
  set(PLATFORM                              "linux")
endif()

set(BUILD                                   "build/${PLATFORM}")

################################################################################
##                                                                            ##
##                                 tree-sitter                                ##
##                                                                            ##
################################################################################
set(TREE_SITTER_DIR                                "${CMAKE_SOURCE_DIR}/3rd/tree-sitter")
set(TREE_SITTER_INCLUDE_DIR                        "${TREE_SITTER_DIR}/lib/include")
set(TREE_SITTER_LIBRARY_DIR                        "${TREE_SITTER_DIR}"
                                                   "${TREE_SITTER_DIR}/test/fixtures/grammars/java")
set(TREE_SITTER_LIBRARIES                          tree-sitter tree-sitter-java)

################################################################################
##                                                                            ##
##                                   argtable3                                ##
##                                                                            ##
################################################################################
set(ARGTABLE3_DIR                           "${CMAKE_SOURCE_DIR}/../iWork-3rd/argtable3-3.3.1")
set(ARGTABLE3_INCLUDE_DIR                   "${ARGTABLE3_DIR}/src")
set(ARGTABLE3_LIBRARY_DIR                   "${ARGTABLE3_DIR}/${BUILD}")
set(ARGTABLE3_LIBRARIES                     "${ARGTABLE3_LIBRARY_DIR}/src/libargtable3.a")

include_directories(
  ${TREE_SITTER_INCLUDE_DIR}
  ${ARGTABLE3_INCLUDE_DIR}
)

link_directories(
  src
  ${TREE_SITTER_LIBRARY_DIR}
  ${ARGTABLE3_LIBRARY_DIR}
)

add_executable(${PROJECT_NAME}
  src/main.c
  src/iw-cfg.c
  src/iw-complex.c
  src/iw-dfg.c
)

target_link_libraries(${PROJECT_NAME}
  ${TREE_SITTER_LIBRARIES}
  ${ARGTABLE3_LIBRARIES}
)
