add_subdirectory(LexParser)
add_subdirectory(Record)
add_subdirectory("System")

add_executable(DB main.cpp)

target_link_libraries(DB Parser)
# target_link_libraries(DB SystemManager)

target_include_directories(DB PUBLIC 
	"${CMAKE_CURRENT_SOURCE_DIR}/LexParser/parser"
)