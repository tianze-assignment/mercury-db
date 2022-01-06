#include <exception>
#include <iostream>
#include <string>
#include "parse.h"
#include "antlr4-runtime.h"

using namespace std;

int main(){
	cout << R""""(
    __  ___                                ____  ____ 
   /  |/  /__  ____________  _________  __/ __ \/ __ )
  / /|_/ / _ \/ ___/ ___/ / / / ___/ / / / / / / __  |
 / /  / /  __/ /  / /__/ /_/ / /  / /_/ / /_/ / /_/ / 
/_/  /_/\___/_/   \___/\__,_/_/   \__, /_____/_____/  
                                 /____/               
)"""" << endl << endl;

	while(true){
		cout << "> ";
		string input, s;
		while(true){
			getline(cin, s);
			if(s.ends_with(';')){
				input.append(s);
				break;
			}
			input.append(s + ' ');
			cout << "... ";
		}
		cout << input <<endl;
		if(input == "q;"){
			cout << "bye~" << endl;
			break;
		}
		antlrcpp::Any r = parse(input);
		if(r.isNull()){ // if syntax error
			cout << "Syntax error" << endl;
			continue;
		}
		for(const antlrcpp::Any& statement : r.as<vector<antlrcpp::Any>>()){
			try{
				cout << statement.as<const char *>() << endl;
			}catch(bad_cast){
				cout << statement.as<int>() << endl;
			}
		}
	}

	// do some close setup
	
}