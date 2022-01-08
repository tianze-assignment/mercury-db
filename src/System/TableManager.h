#pragma once

#include <unordered_map>

#include "RecordHandler.h"
#include "IndexHandler.h"

#include "DBManager.h"
#include "Schema.h"

class TableManager {
    DBManager *db_manager;
    RecordHandler *record_handler;
    IndexHandler *index_handler;

    unordered_map<string, Schema> schemas;

	Schema *get_schema(string name);

   public:
    TableManager(DBManager *);
    ~TableManager();

    string create_table(Schema &schema);
	string drop_table(string name);
	string describe_table(string name);

	string insert(string table_name, vector<vector<Value>> &value_lists);

};