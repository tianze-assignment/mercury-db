#include "DBManager.h"

#include <filesystem>

#include "variadic_table/VariadicTable.h"

namespace fs = std::filesystem;

fs::path DBManager::db_dir(DB_DIR);

DBManager::DBManager() {
    record_handler = new RecordHandler();
    index_handler = new IndexHandler();
}

DBManager::~DBManager() {
    delete record_handler;
    delete index_handler;
}

string DBManager::create_db(string &name) {
    std::error_code code;
    bool suc = fs::create_directories(db_dir / name, code);
    if (suc) return "Created";
    if (code.value() == 0) return "Database already exists";
    return code.message();
}

string DBManager::drop_db(string &name) {
    std::error_code code;
    auto suc = fs::remove_all(db_dir / name, code);
    if (suc) return "Removed";
    if (code.value() == 0) return "Database does not exist";
    return code.message();
}

string DBManager::show_dbs() {
    if (!fs::exists(db_dir)) return "";
    string dbs;
    for (auto e : fs::directory_iterator{db_dir}) {
        if (e.is_directory()) dbs.append(e.path().filename().string() + "\n");
    }
    return dbs;
}

string DBManager::use_db(string &name) {
    if (!fs::exists(db_dir / name)) return "Database does not exist";

    this->schemas.clear();

    this->current_dbname = name;

    for (auto e : fs::directory_iterator{db_dir / name}) {
        if (e.is_directory()) {
            string tableName = e.path().filename().string();
            this->schemas[tableName] = Schema(tableName, current_dbname);
        }
    }
    
    return "Using " + name;
}

string DBManager::show_tables() {
    if (this->current_dbname.empty()) return "Please use a database first";
    VariadicTable<std::string> vt({"Tables in " + this->current_dbname});
    for (auto e : fs::directory_iterator{db_dir / this->current_dbname}) {
        if (e.is_directory()) vt.addRow(e.path().filename().string());
    }
    vt.print(std::cout);
    return "";
}

string DBManager::show_indexes() {
    return "Not supported yet";
}

