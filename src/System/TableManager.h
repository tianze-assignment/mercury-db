#pragma once

#include <unordered_map>

#include "DBManager.h"
#include "Schema.h"

class TableManager {
    DBManager *db_manager;
    unordered_map<string, Schema> schemas;

	Schema *get_schema(string name);

   public:
    TableManager(DBManager *);
    ~TableManager();

    string create_table(Schema &schema);
	string drop_table(string name);
	string describe_table(string name);
};