#pragma once

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <filesystem>

#include "RecordHandler.h"
#include "IndexHandler.h"
#include "Schema.h"
#include "Query.h"

using namespace std;

#define DB_DIR "databases"
#define MANAGER_NAME "MercuryDB"

typedef map<string,int> NameMap;

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

    void check_db();
    void check_db_empty();

    string file_name(const Schema& schema);
    void open_record(const Schema& schema);
    Schema& get_schema(const string& table_name);
    Record to_record(const vector<Value>& value_list, const Schema& schema);
    vector<Value> to_value_list(const Record& record, const Schema& schema);
    void check_column(const string& table_name, const NameMap& column_map, const QueryCol& col);
    void check_column(const NameMap& table_map, const vector<NameMap>& column_maps, const QueryCol& col);
    Value get_value(const vector<vector<Value>>& value_lists,
            const NameMap& table_map, const vector<NameMap>& column_maps, const QueryCol& col);

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

    static string rows_text(int row);
	string insert(string table_name, vector<vector<Value>> &value_lists);
    string delete_(string table_name, vector<Condition> conditions);
    string update(string table_name, vector<pair<string,Value>> assignments, vector<Condition> conditions);
    Query select(vector<QueryCol> cols, vector<string> tables, vector<Condition> conditions);

    string alter_add_index(string &table_name, vector<string> &fields);
    string alter_drop_index(string &table_name, vector<string> &fields);
    string alter_drop_pk(string &table_name, string &pk_name);
};
