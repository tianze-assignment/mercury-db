#include <filesystem>
#include <unordered_set>

#include "DBManager.h"

namespace fs = std::filesystem;

string DBManager::file_name(const Schema& schema) {
    return string(DB_DIR) + "/" + current_dbname + "/" + schema.table_name + "/" + schema.table_name;
}

void DBManager::open_record(const Schema& schema) {
    record_handler->openFile((file_name(schema) + "data").data(), schema.record_type());
}

string DBManager::rows_text(int row) {
    return to_string(row) + " row" + (row > 1 ? "s" : "");
}

string DBManager::create_table(Schema &schema) {
    // check use database
    if (current_dbname.empty()) return "Please use a database first";
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

        for (auto &fk_ref_col : fk.ref_fks) {
            if (ref_table_schema.find_column(fk_ref_col) == ref_table_schema.columns.size())
                return "Foreign key ref field not found: " + fk_ref_col;
        }
    }
    // create directory
    std::error_code code;
    bool suc = fs::create_directories(db_dir / current_dbname / schema.table_name, code);
    if (!suc) {
        if (code.value() == 0) return "Table already exists";
        return code.message();
    }
    // write
    suc = schema.write(current_dbname);
    if (!suc) return "Writing failed";

    this->schemas[schema.table_name] = schema;

    record_handler->createFile((file_name(schema) + ".data").data(), schema.record_type());

    return "Created";
}

string DBManager::drop_table(string name){
	// check use database
    if (current_dbname.empty()) return "Please use a database first";
	// check table
	auto dir = db_dir / current_dbname / name;
	std::error_code code;
	auto suc = fs::remove_all(dir, code);
	if(suc) return "Removed";
	if(code.value() == 0) return "Table does not exist";
	return code.message();
}

string DBManager::describe_table(string name){
	// check use database
    if (current_dbname.empty()) return "Please use a database first";

	if(schemas.find(name) == schemas.end()) return "Table does not exist";
	return schemas[name].to_str();
}

string DBManager::insert(string table_name, vector<vector<Value>> &value_lists){
	// check use database
    if (current_dbname.empty()) return "Please use a database first";

	if(schemas.find(table_name) == schemas.end()) return "Table does not exist";
    Schema& schema = schemas[table_name];
    RecordType type = schema.record_type();
    open_record(schema);

    for(auto value_list: value_lists) {
        if (value_list.size() != schema.columns.size()) return "Invalid number of values";
        Record record(type);
        int int_count = 0, varchar_count = 0;
        for (int i = 0; i < schema.columns.size(); ++i) {
            auto& column = schema.columns[i];
            auto& value = value_list[i];
            if (value.type == NULL_TYPE) {
                if (column.not_null) return (string)"Column """ + column.name + """ should not be NULL";
                if (column.type == VARCHAR) record.varchar_null[varchar_count++] = true;
                else record.int_null[int_count++] = true; 
            }
            else if (value.type != column.type) return (string)"Invalid value type of Column """ + column.name + """";
            else if (value.type == VARCHAR) {
                record.varchar_null[varchar_count] = false;
                record.varchar_data[varchar_count++] = (char*)value.bytes.data();
            }
            else {
                record.int_null[int_count] = false;
                record.int_data[int_count++] = *((int*)value.bytes.data());
            }
        }
        record_handler->ins(record);
    }
    return "Insert " + rows_text(value_lists.size()) + " OK";
}