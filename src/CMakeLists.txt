add_subdirectory(FileSystem)
add_subdirectory(LexParser)
add_subdirectory(Record)
add_subdirectory(Index)

add_executable(DB testIndex.cpp)

target_link_libraries(DB Parser)
target_link_libraries(DB Record)
target_link_libraries(DB Index)

target_include_directories(DB PUBLIC 
	"${CMAKE_CURRENT_SOURCE_DIR}/LexParser/parser"
)