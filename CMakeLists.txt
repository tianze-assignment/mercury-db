cmake_minimum_required(VERSION 3.16)

project(MercuryDB)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_subdirectory(src)

# Disable building tests and examples in libfort project
set(FORT_ENABLE_TESTING OFF CACHE INTERNAL "")

add_subdirectory(third-party/libfort)
