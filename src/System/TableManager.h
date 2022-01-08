#pragma once

#include <unordered_map>

#include "DBManager.h"
#include "Schema.h"

class TableManager {
    DBManager *db_manager;
    unordered_map<string, Schema> schemas;

   public:
    TableManager(DBManager *);
    ~TableManager();

    string create_table(Schema &schema);
};