cmake_minimum_required(VERSION 3.16)

project(MercuryDB)

set(CMAKE_CSS_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_executable(DB testfilesystem.cpp)

target_include_directories(DB PUBLIC "${PROJECT_SOURCE_DIR}/src")