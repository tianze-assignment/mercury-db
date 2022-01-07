add_subdirectory(FileSystem)
add_subdirectory(LexParser)
add_subdirectory(Record)

add_executable(DB main.cpp)

target_link_libraries(DB Parser)
target_link_libraries(DB Record)

target_include_directories(DB PUBLIC 
	"${CMAKE_CURRENT_SOURCE_DIR}/LexParser/parser"
)