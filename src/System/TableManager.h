#pragma once

#include "DBManager.h"

class TableManager {
    DBManager *db_manager;

   public:
    TableManager(DBManager *);

	string create_table();
};