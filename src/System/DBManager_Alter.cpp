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
    FileSystem::save();
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
    FileSystem::save();
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

string DBManager::alter_add_fk(string &table_name, string &fk_name, string &ref_table_name, vector<string> &fields, vector<string> &ref_fields) {
    check_db();
    auto &schema = get_schema(table_name);
    // duplicate name
    for (auto fk : schema.fks) {
        if (fk.name == fk_name) throw DBException("Duplicate fk name: " + fk_name);
    }
    // check fks are among the fields
    vector<int> column_indexes;
    for (auto &fk : fields) {
        int i = schema.find_column(fk);
        if (i == schema.columns.size()) throw DBException(fmt("Column '%s' not found in schema", fk.c_str()));
        column_indexes.push_back(i);
        if (schema.columns[i].type != INT) throw DBException("Only INT type is supported");
    }
    // check ref_fields is the other table's pk
    auto &ref_schema = get_schema(ref_table_name);
    if (ref_schema.pk.pks != ref_fields) throw DBException("Ref field is not the pk of the referenced table");
    // assuming current table's fk and ref table's pk can only be INT
    // check fk constraint
    int ref_index_num = find(ref_schema.indexes.begin(), ref_schema.indexes.end(), ref_fields) - ref_schema.indexes.begin();
    if (ref_index_num == ref_schema.indexes.size()) throw DBException("Ref field doesn't have corresponding index");
    index_handler->openIndex(
        (db_dir / current_dbname / ref_table_name / (ref_table_name + to_string(ref_index_num) + ".index")).c_str(), ref_fields.size());
    open_record(schema);
    vector<int> ref_column_indexes;
    for (auto &c : ref_fields) {
        ref_column_indexes.push_back(ref_schema.find_column(c));
    }
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
        auto it = index_handler->lowerBound(ints.data());
        open_record(ref_schema);
        auto record = *(RecordHandler::Iterator(record_handler, *it));
        auto ref_values = to_value_list(record, ref_schema);
        vector<int> ref_ints;
        for (auto &ref_column_index : ref_column_indexes) {
            ref_ints.push_back(*((int *)(&ref_values[ref_column_index].bytes[0])));
        }
        if (ints != ref_ints) {
            stringstream ss;
            copy(ints.begin(), ints.end(), ostream_iterator<int>(ss, ", "));
            throw DBException(fmt("Field (%s) in current table is not found in ref table", ss.str().c_str()));
        }

        open_record(schema);
    }

    return "Added";
}
