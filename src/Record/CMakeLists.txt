file(GLOB_RECURSE SRC "*.cpp" "*.h")

add_library(Record STATIC ${SRC})

target_include_directories(Record PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/../FileSystem"
)