#pragma once

#include <string>
#include <vector>

using namespace std;

#define DB_DIR "databases"

class DBManager {
    string current_db;

   public:
    string get_current_db();
    string create_db(string &name);
    string drop_db(string &name);
    string show_dbs();
    string use_db(string &name);
    string show_tables();
    string show_indexes();
};
