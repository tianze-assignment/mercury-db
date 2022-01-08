#include <string>
#include "antlr4-runtime.h"
#include "DBManager.h"

antlrcpp::Any parse(std::string sSQL, DBManager *db_manager);
