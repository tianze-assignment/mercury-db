file(GLOB_RECURSE SRC "*.cpp" "*.h")

add_library(Index STATIC ${SRC})

target_link_libraries(Index FileSystem)
target_link_libraries(Index Record)

target_include_directories(Index PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
)