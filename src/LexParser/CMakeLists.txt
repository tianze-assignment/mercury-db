file(GLOB_RECURSE SRC "*.cpp")
# set(SRC parser/SQLBaseVisitor.cpp parser/SQLLexer.cpp parser/SQLParser.cpp parser/SQLVisitor.cpp parse.cpp MyVisitor.cpp)

add_library(Parser STATIC ${SRC})

target_link_libraries(Parser "${CMAKE_CURRENT_SOURCE_DIR}/antlr_runtime/lib/libantlr4-runtime.a")
target_link_libraries(Parser SystemManager)

target_include_directories(Parser PUBLIC 
	"${CMAKE_CURRENT_SOURCE_DIR}"
	"${CMAKE_CURRENT_SOURCE_DIR}/parser"
	"${CMAKE_CURRENT_SOURCE_DIR}/antlr_runtime/include"
)