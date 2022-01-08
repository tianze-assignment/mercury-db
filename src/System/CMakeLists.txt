file(GLOB_RECURSE SRC "*.cpp" "*.h")

add_library(SystemManager STATIC ${SRC})

target_link_libraries(SystemManager Record)
target_link_libraries(SystemManager Index)

target_link_libraries(SystemManager fort)

target_include_directories(SystemManager PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${PROJECT_SOURCE_DIR}/lib"
)