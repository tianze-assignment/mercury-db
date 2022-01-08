#include <exception>
#include <iostream>
#include <string>

#include "DBManager.h"
#include "TableManager.h"
#include "antlr4-runtime.h"
#include "parse.h"

using namespace std;

int main() {
    cout << R""""(
    __  ___                                ____  ____ 
   /  |/  /__  ____________  _________  __/ __ \/ __ )
  / /|_/ / _ \/ ___/ ___/ / / / ___/ / / / / / / __  |
 / /  / /  __/ /  / /__/ /_/ / /  / /_/ / /_/ / /_/ / 
/_/  /_/\___/_/   \___/\__,_/_/   \__, /_____/_____/  
                                 /____/               
)"""" << endl
         << endl;

    DBManager* db_manager = new DBManager();
    TableManager* table_manager = new TableManager(db_manager);

    while (true) {
        string current_db = db_manager->get_current_db();
        string indent_str = current_db.empty() ? "MecuryDB" : current_db;
        int indent_len = indent_str.length();
        cout << indent_str << "> ";

        string input, s;
        while (true) {
            getline(cin, s);
            if (s.ends_with(';')) {
                input.append(s);
                break;
            }
            input.append(s + ' ');
            for (int i = 0; i < indent_len; i++) cout << " ";
            cout << "> ";
        }
        if (input == "q;") {
            cout << "bye~" << endl;
            break;
        }
        antlrcpp::Any r = parse(input, db_manager, table_manager);
        if (r.isNull()) {  // if syntax error
            cout << "Syntax error" << endl;
            continue;
        }
        for (const antlrcpp::Any& statement : r.as<vector<antlrcpp::Any>>()) {
            try {
                cout << statement.as<const char*>() << endl;
            } catch (bad_cast) {
                cout << statement.as<int>() << endl;
            }
        }
    }

    // do some close setup
    delete table_manager;
    delete db_manager;
}