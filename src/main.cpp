#include <exception>
#include <iostream>
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
		string s;
		getline(cin, s);
		if(s == "quit"){
			cout << "bye~" << endl;
			break;
		}
		antlrcpp::Any r = parse(s);
		for(const antlrcpp::Any& statement : r.as<vector<antlrcpp::Any>>()){
			try{
				cout << statement.as<const char *>() << endl;
			}catch(bad_cast){
				cout << statement.as<int>() << endl;
			}
		}
	}
	
}