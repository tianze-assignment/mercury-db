#include <exception>
#include <iostream>

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
    TableManager *table_manager = new TableManager(db_manager);

    while (true) {
        string current_db = db_manager->get_current_db();
        cout << (current_db.empty() ? "MecuryDB" : current_db) << "> ";
        string s;
        getline(cin, s);
        if (s == "quit") {
            cout << "bye~" << endl;
            break;
        }
        antlrcpp::Any r = parse(s, db_manager, table_manager);
        for (const antlrcpp::Any& statement : r.as<vector<antlrcpp::Any>>()) {
            try {
                cout << statement.as<const char*>() << endl;
            } catch (bad_cast) {
                cout << statement.as<int>() << endl;
            }
        }
    }

    delete table_manager;
    delete db_manager;
}