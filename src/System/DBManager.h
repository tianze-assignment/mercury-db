#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>

#include "RecordHandler.h"
#include "IndexHandler.h"
#include "Schema.h"
#include "Query.h"

using namespace std;

#define DB_DIR "databases"

class DBException : public exception {
    string message;
public:
    DBException(string message):message(message){}
    const char* what() const throw() {
        return message.data();
    }
};

class DBManager {
    static filesystem::path db_dir;
    RecordHandler *record_handler;
    IndexHandler *index_handler;
    unordered_map<string, Schema> schemas;
    string file_name(const Schema& schema);
    void open_record(const Schema& schema);
    string rows_text(int row);
    void check_db();
    Schema& get_schema(const string& table_name);

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
    Query select(vector<QueryCol> cols, vector<string> tables, vector<Condition> conds);
};
