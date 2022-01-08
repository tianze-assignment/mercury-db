#include "TableManager.h"

#include <filesystem>
#include <unordered_set>

TableManager::TableManager(DBManager *db_manager) : db_manager(db_manager) {}
TableManager::~TableManager() {
    for (auto i : schemas) {
        i.second.write(db_manager->get_current_db());
    }
}

namespace fs = std::filesystem;
fs::path db_root(DB_DIR);

Schema *TableManager::get_schema(string name) {
    if (this->schemas.find(name) == this->schemas.end()) {  // if not found
        // read from file
        if (fs::exists(db_root / db_manager->get_current_db() / name / (name + ".schema"))) {  // if file exists
            this->schemas[name] = Schema(name, db_manager->get_current_db());
            return &this->schemas[name];
        } else {  // if file not found
            return nullptr;
        }
    } else {  // if found
        return &this->schemas[name];
    }
}

string TableManager::create_table(Schema &schema) {
    // check use database
    if (db_manager->get_current_db().empty()) return "Please use a database first";
    // check schema
    unordered_set<string> column_names;
    for (auto &column : schema.columns) {
        if (column_names.find(column.name) == column_names.end())
            column_names.insert(column.name);
        else
            return "Duplicate column name: " + column.name;
    }
    for (auto &pk_column : schema.pk.pks) {
        if (column_names.find(pk_column) == column_names.end()) return "Primary key field not declared: " + pk_column;
    }
    for (auto &fk : schema.fks) {
        for (auto &fk_column : fk.fks) {
            if (column_names.find(fk_column) == column_names.end()) return "Foreign key field not declared: " + fk_column;
        }
        Schema *ref_table_schema = get_schema(fk.ref_table);
		if(!ref_table_schema) return "Foreign key ref table not found: " + fk.ref_table;

        for (auto &fk_ref_col : fk.ref_fks) {
            if (ref_table_schema->find_column(fk_ref_col) == ref_table_schema->columns.size())
                return "Foreign key ref field not found: " + fk_ref_col;
        }
    }
    // create directory
    std::error_code code;
    bool suc = fs::create_directories(db_root / db_manager->get_current_db() / schema.table_name, code);
    if (!suc) {
        if (code.value() == 0) return "Table already exists";
        return code.message();
    }
    // write
    suc = schema.write(db_manager->get_current_db());
    if (!suc) return "Writing failed";

    this->schemas[schema.table_name] = schema;
    return "Created";
}