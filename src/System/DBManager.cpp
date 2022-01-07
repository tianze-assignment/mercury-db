#include "DBManager.h"

#include <filesystem>

#include "variadic_table/VariadicTable.h"

string DBManager::get_current_db() {
    return this->current_db;
}

namespace fs = std::filesystem;
fs::path db_dir(DB_DIR);

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
    this->current_db = name;
    return "Using " + name;
}

string DBManager::show_tables() {
    if (this->current_db.empty()) return "Please use a database first";
    VariadicTable<std::string> vt({"Tables in " + this->current_db});
    for (auto e : fs::directory_iterator{db_dir / this->current_db}) {
        if (e.is_directory()) vt.addRow(e.path().filename().string());
    }
    vt.print(std::cout);
    return "";
}

string DBManager::show_indexes() {
    return "Not supported yet";
}