file(GLOB_RECURSE SRC "*.cpp" "*.h")

add_library(FileSystem STATIC ${SRC})

target_include_directories(FileSystem PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
)