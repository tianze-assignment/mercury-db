#include "parse.h"
#include "antlr4-runtime.h"

int main(){
	int r = parse("SELECT * FROM t;");
}