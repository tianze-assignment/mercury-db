#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>

#include "RecordHandler.h"
#include "IndexHandler.h"
#include "Schema.h"

using namespace std;

#define DB_DIR "databases"

class DBManager {
    static filesystem::path db_dir;
    RecordHandler *record_handler;
    IndexHandler *index_handler;
    unordered_map<string, Schema> schemas;

   public:
    DBManager();
    ~DBManager();

    string current_dbname;
    string create_db(string &name);
    string drop_db(string &name);
    string show_dbs();
    string use_db(string &name);
    string show_tables();
    string show_indexes();

    string create_table(Schema &schema);
	string drop_table(string name);
	string describe_table(string name);

	string insert(string table_name, vector<vector<Value>> &value_lists);
};
