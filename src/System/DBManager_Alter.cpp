#include <iterator>
#include <unordered_set>

#include "DBManager.h"
#include "VectorHash.h"
#include "fmt.h"

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

string DBManager::alter_drop_pk(string &table_name, string &pk_name) {
    check_db();
    auto &schema = get_schema(table_name);
    if (schema.pk.pks.empty()) throw DBException("No primary key");
    if (!pk_name.empty() && schema.pk.name != pk_name) {
        string info = fmt("Primary key name '%s' not equal to the original name '%s'", pk_name.c_str(), schema.pk.name.c_str());
        throw DBException(info);
    }

    // delete corresponding index
    if (find(schema.indexes.begin(), schema.indexes.end(), schema.pk.pks) != schema.indexes.end())
        this->alter_drop_index(table_name, schema.pk.pks);
    // delete pk
    schema.pk.pks.clear();
    // write
    bool suc = schema.write(current_dbname);
    if (!suc) throw DBException("Write schema failed");
    return "Primary key dropped";
}

string DBManager::alter_drop_fk(string &table_name, string &fk_name) {
    check_db();
    auto &schema = get_schema(table_name);
    int i = schema.find_fk_by_name(fk_name);
    if (i == schema.fks.size()) throw DBException(fmt("No foreign key '%s'", fk_name.c_str()));
    schema.fks.erase(schema.fks.begin() + i);
    bool suc = schema.write(current_dbname);
    if (!suc) throw DBException("Write schema failed");
    return "Foreign key dropped";
}

string DBManager::alter_add_pk(string &table_name, string &pk_name, vector<string> &pks) {
    check_db();
    auto &schema = get_schema(table_name);
    if (!schema.pk.pks.empty()) throw DBException("Primary key already exists");
    // check pks are among the fields
    vector<int> column_indexes;
    bool all_int = true;
    for (auto &pk : pks) {
        int i = schema.find_column(pk);
        if (i == schema.columns.size()) throw DBException(fmt("Column '%s' not found in schema", pk.c_str()));
        column_indexes.push_back(i);
        if (schema.columns[i].type != INT) all_int = false;
    }

    // check unique & null
    unordered_set<vector<int>, VectorHash> pk_values;
    open_record(schema);
    for (auto i = record_handler->begin(); !i.isEnd(); ++i) {
        auto values = to_value_list(*i, schema);
        vector<int> ints;
        for (auto &column_index : column_indexes) {
            if (values[column_index].type == NULL_TYPE)
                throw DBException("ERROR: NULL values found");
            ints.push_back(*((int *)(&values[column_index].bytes[0])));
        }
        if (pk_values.find(ints) != pk_values.end()) {
            stringstream ss;
            copy(ints.begin(), ints.end(), ostream_iterator<int>(ss, " "));
            throw DBException("Found duplicate field tuples:\n" + ss.str());
        }
        pk_values.insert(ints);
    }

    schema.pk.name = pk_name;
    schema.pk.pks = pks;
    bool suc = schema.write(current_dbname);
    if (!suc) throw DBException("Write schema failed while adding primary key");

    // build index
    bool had_index = find(schema.indexes.begin(), schema.indexes.end(), pks) != schema.indexes.end();
    if (!had_index && all_int) {
        alter_add_index(table_name, pks);
    }

    return "Added";
}
