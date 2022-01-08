file(GLOB_RECURSE SRC "*.cpp" "*.h")

add_library(Record STATIC ${SRC})

target_link_libraries(Record FileSystem)

target_include_directories(Record PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
)