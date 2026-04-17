cmake_minimum_required(VERSION 3.16)
project(iw-llmclient C)

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
##                                   argtable3                                ##
##                                                                            ##
################################################################################
set(ARGTABLE3_DIR                           "${CMAKE_SOURCE_DIR}/../iWork-3rd/argtable3-3.3.1")
set(ARGTABLE3_INCLUDE_DIR                   "${ARGTABLE3_DIR}/src")
set(ARGTABLE3_LIBRARY_DIR                   "${ARGTABLE3_DIR}/${BUILD}")
set(ARGTABLE3_LIBRARIES                     "${ARGTABLE3_LIBRARY_DIR}/src/libargtable3.a")

################################################################################
##                                                                            ##
##                                     curl                                   ##
##                                                                            ##
################################################################################
set(CURL_DIR                              "${CMAKE_SOURCE_DIR}/../iWork-3rd/curl-8.19.0")
set(CURL_INCLUDE_DIR                      "${CURL_DIR}/include")
set(CURL_LIBRARY_DIR                      "${CURL_DIR}/${BUILD}")
set(CURL_LIBRARIES                        curl)

include_directories(
  ${ARGTABLE3_INCLUDE_DIR}
  ${CURL_INCLUDE_DIR}
)

link_directories(
  src
  ${ARGTABLE3_LIBRARY_DIR}
  ${CURL_LIBRARY_DIR}
)

add_executable(${PROJECT_NAME}
  src/main.c
)

target_link_libraries(${PROJECT_NAME}
  ${ARGTABLE3_LIBRARIES}
  ${CURL_LIBRARIES}
)
