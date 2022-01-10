#include "DBManager.h"

namespace fs = std::filesystem;

string DBManager::alter_add_index(string &table_name, vector<string> &fields) {
    check_db();
    auto &schema = get_schema(table_name);
    // check fields are in schema columns
    vector<int> column_indexes;
    for (auto &field : fields) {
        int column_index = schema.find_column(field);
        if (column_index == schema.columns.size())
            throw DBException("There is no field '" + field + "' in the schema");
        if (schema.columns[column_index].type != INT)
            throw DBException("Field '" + field + "' is not INT");
        column_indexes.push_back(column_index);
    }
    // alter
    schema.indexes.push_back(fields);
    // write schema
    bool suc = schema.write(current_dbname);
    if (!suc) {
        schema.indexes.pop_back();
        throw DBException("Cannot write to schema file");
    }
    // write index
    auto table_path = db_dir / current_dbname / table_name;
    auto index_path = table_path / (table_name + to_string(schema.indexes.size() - 1) + ".index");
    index_handler->createIndex(index_path.c_str(), fields.size());
    auto data_path = table_path / (table_name + ".data");
    open_record(schema);
    for (auto i = record_handler->begin(); !i.isEnd(); ++i) {
        auto values = to_value_list(*i, schema);
        vector<int> ints;
        bool has_null = false;
        for (auto &column_index : column_indexes) {
            if (values[column_index].type == NULL_TYPE) {
                has_null = true;
                break;
            }
            ints.push_back(*((int *)(&values[column_index].bytes[0])));
        }
        if (has_null) continue;
        index_handler->ins(ints.data(), i.toInt());
    }
    return "Added";
}

string DBManager::alter_drop_index(string &table_name, vector<string> &fields) {
    check_db();
    auto &schema = get_schema(table_name);
    auto &indexes = schema.indexes;
    // check index
    auto it = find(indexes.begin(), indexes.end(), fields);
    if (it == indexes.end())
        throw DBException("Index not found");
    int pos = it - indexes.begin();
    // delete
    indexes.erase(it);
    // write schema
    bool suc = schema.write(current_dbname);
    if (!suc) {
        indexes.insert(it, fields);
        throw DBException("Write schema failed");
    }
    // update index filenames
    auto table_path = db_dir / current_dbname / table_name;
    std::error_code err;
	// Is it ok to delete index file directly?
    fs::remove(table_path / (table_name + to_string(pos) + ".index"), err);
    if (err.value() != 0) throw DBException(err.message());
    int max_pos = indexes.size();
    for (int i = pos + 1; i <= max_pos; i++) {
        fs::rename(
            table_path / (table_name + to_string(i) + ".index"),
            table_path / (table_name + to_string(i - 1) + ".index"),
            err);
        if (err.value() != 0) throw DBException(err.message());
    }
	return "Dropped";
}