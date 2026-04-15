cmake_minimum_required(VERSION 3.16)
project(iw-filemerge VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if (WIN32)
elseif (APPLE)
  set(PLATFORM                              "darwin")
elseif (UNIX)
  set(PLATFORM                              "linux")
endif()

set(BUILD                                   "build/${PLATFORM}")

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
  src
  ${ARGTABLE3_INCLUDE_DIR}
)

add_executable(${PROJECT_NAME}
  src/main.c
  src/merge.c
)

target_link_libraries(${PROJECT_NAME} PRIVATE 
  ${ARGTABLE3_LIBRARIES}
)
