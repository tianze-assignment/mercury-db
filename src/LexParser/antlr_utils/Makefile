# generate lexer & parser code

all: SQL.g4
	java -jar ./antlr.jar -Dlanguage=Cpp ./SQL.g4 -visitor -no-listener -o ./parser
