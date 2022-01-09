#include <filesystem>
#include <unordered_set>
#include "fort.hpp"

#include "DBManager.h"

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

void DBManager::check_db() {
    if (current_dbname.empty()) throw DBException("Please use a database first");
}

void DBManager::check_db_empty() {
    if (!current_dbname.empty()) throw DBException(string("Please input command \"USE ") + MANAGER_NAME + "\" first");
}

string DBManager::create_db(string &name) {
    if (name == MANAGER_NAME) throw DBException("Invalid database name");
    std::error_code code;
    bool suc = fs::create_directories(db_dir / name, code);
    if (suc) return "Created";
    if (code.value() == 0) return "Database already exists";
    return code.message();
}

string DBManager::drop_db(string &name) {
    check_db_empty();
    if (name == MANAGER_NAME) throw DBException("Invalid database name");
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
    if (name == MANAGER_NAME) {
        this->schemas.clear();
        this->current_dbname = "";
        return string("Back to ") + MANAGER_NAME;
    }

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
    check_db();
    fort::char_table table;
    table << fort::header
        << "Tables in " + this->current_dbname << fort::endr;
    for(auto sch : this->schemas)
        table << sch.second.table_name << fort::endr;
    return table.to_string();
}

string DBManager::show_indexes() {
    return "Not supported yet";
}

string DBManager::create_table(Schema &schema) {
    check_db();
    // check schema
    unordered_set<string> column_names;
    for (auto &column : schema.columns) {
        if (column_names.find(column.name) == column_names.end())
            column_names.insert(column.name);
        else
            return "Duplicate column name: " + column.name;
		if(column.has_default){
			if(column.default_value.type == NULL_TYPE){
				if(column.not_null) return "Default is NULL but NOT NULL is enabled";
			}else{
				if(column.type != column.default_value.type) return "Default value type and field type of column '" + column.name + "' are not identical";
			}
		}
    }
    for (auto &pk_column : schema.pk.pks) {
        if (column_names.find(pk_column) == column_names.end()) return "Primary key field not declared: " + pk_column;
    }
    for (auto &fk : schema.fks) {
        for (auto &fk_column : fk.fks) {
            if (column_names.find(fk_column) == column_names.end()) return "Foreign key field not declared: " + fk_column;
        }
        if (schemas.find(fk.ref_table) == schemas.end()) return "Foreign key ref table not found: " + fk.ref_table;
        Schema &ref_table_schema = schemas[fk.ref_table]; 

        if(fk.ref_fks != ref_table_schema.pk.pks) return "Foreign key ref columns do not match primary key";
    }
    // create directory
    std::error_code code;
    bool suc = fs::create_directories(db_dir / current_dbname / schema.table_name, code);
    if (!suc) {
        if (code.value() == 0) return "Table already exists";
        return code.message();
    }
    // add index for primary key if all fields are INT
    bool pk_all_int = schema.pk.pks.empty() ? false : true;
    for (auto &pk_column : schema.pk.pks) {
        if(schema.columns[schema.find_column(pk_column)].type != INT)
            pk_all_int = false;
    }
    if(pk_all_int){
        schema.indexes.push_back(schema.pk.pks);
        auto index_path = db_dir/current_dbname/schema.table_name/(schema.table_name+"0"+".index"); // "0" corresponds to index at vector indexes
        index_handler->createIndex(index_path.c_str(), schema.pk.pks.size());
    }
    // write
    suc = schema.write(current_dbname);
    if (!suc) return "Writing failed";

    this->schemas[schema.table_name] = schema;

    record_handler->createFile((file_name(schema) + ".data").data(), schema.record_type());
    FileSystem::save();

    return "Created";
}

string DBManager::drop_table(string name){
    check_db();
	// check table
	auto dir = db_dir / current_dbname / name;
	std::error_code code;
	auto suc = fs::remove_all(dir, code);
	if(suc) return "Removed";
	if(code.value() == 0) return "Table does not exist";

    schemas.erase(schemas.find(name));

	return code.message();
}

string DBManager::describe_table(string name){
    check_db();
    return get_schema(name).to_str();
}
