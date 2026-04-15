cmake_minimum_required(VERSION 3.10)

project(iw-sourcewrite VERSION 1.0)

# Set C standard
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
##                                     md4c                                   ##
##                                                                            ##
################################################################################
set(MD4C_DIR                                "${CMAKE_SOURCE_DIR}/../iWork-3rd/md4c-0.5.2")
set(MD4C_INCLUDE_DIR                        "${MD4C_DIR}/src")
set(MD4C_LIBRARY_DIR                        "${MD4C_DIR}/${BUILD}/src")
set(MD4C_LIBRARIES                          "${MD4C_LIBRARY_DIR}/libmd4c.dylib")

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
  ${MD4C_INCLUDE_DIR}
  ${ARGTABLE3_INCLUDE_DIR}
)

link_directories(
  ${MD4C_LIBRARY_DIR}
  ${ARGTABLE3_LIBRARY_DIR}
)

add_executable(${PROJECT_NAME} 
  src/main.c
)

target_link_libraries(${PROJECT_NAME} PRIVATE 
  ${MD4C_LIBRARIES}
  ${ARGTABLE3_LIBRARIES}
)

