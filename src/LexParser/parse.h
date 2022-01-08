#include <string>
#include "antlr4-runtime.h"
#include "DBManager.h"
#include "TableManager.h"

antlrcpp::Any parse(std::string sSQL, DBManager *db_manager, TableManager *table_manager);
