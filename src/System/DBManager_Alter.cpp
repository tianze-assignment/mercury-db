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
    if (index_handler->createIndex(index_path.c_str(), fields.size()))
        throw DBException("Create file failed");
    open_record(schema);
    RecordType record_type = schema.record_type();
    for (auto i = record_handler->begin(); !i.isEnd(); ++i) {
        auto record = *i;
        auto values = to_value_list(record, schema);
        record.release(record_type);
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
    FileSystem::save();
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
	// check fk of other tables
	for(auto i : schemas){
		for(auto &fk : i.second.fks){
			if(fk.ref_table == table_name && fk.ref_fks == schema.pk.pks){
				throw DBException(fmt("Table '%s' has fk '%s' referencing current table's pk", i.first.c_str(), fk.name.c_str()));
			}
		}
	}

    // delete corresponding index
    FileSystem::save();
    fs::remove(db_dir / current_dbname / table_name / (table_name + "_pk.index"));
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
	// delete index
    FileSystem::save();
	fs::remove(db_dir / current_dbname / table_name / (table_name + "_" + schema.fks[i].name + ".index"));
	// delete fk
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
    for (auto &pk : pks) {
        int i = schema.find_column(pk);
        if (i == schema.columns.size()) throw DBException(fmt("Column '%s' not found in schema", pk.c_str()));
        if (schema.columns[i].type != INT) throw DBException("Primary key only support INT");
		column_indexes.push_back(i);
    }

    // check unique & null, build index
    unordered_set<vector<int>, VectorHash> pk_values;
	auto table_path = db_dir / current_dbname / table_name;
    auto index_path = table_path / (table_name + "_pk.index");
    if ( index_handler->createIndex(index_path.c_str(), pks.size()))
        throw DBException("Create file failed");
    open_record(schema);
    RecordType record_type = schema.record_type();
    for (auto i = record_handler->begin(); !i.isEnd(); ++i) {
        auto record = *i;
        auto values = to_value_list(record, schema);
        record.release(record_type);
        vector<int> ints;
        for (auto &column_index : column_indexes) {
            if (values[column_index].type == NULL_TYPE){
				FileSystem::save();
				fs::remove(index_path);
                throw DBException("ERROR: NULL values found");
			}
            ints.push_back(*((int *)(&values[column_index].bytes[0])));
        }
        if (pk_values.find(ints) != pk_values.end()) {
			FileSystem::save();
			fs::remove(index_path);
            stringstream ss;
            copy(ints.begin(), ints.end(), ostream_iterator<int>(ss, " "));
            throw DBException("Found duplicate field tuples:\n" + ss.str());
        }
        pk_values.insert(ints);
		index_handler->ins(ints.data(), i.toInt());
    }
	FileSystem::save();

    schema.pk.name = pk_name;
    schema.pk.pks = pks;
    bool suc = schema.write(current_dbname);
    if (!suc) throw DBException("Write schema failed while adding primary key");

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
	
    open_record(schema);
    vector<int> ref_column_indexes;
    for (auto &c : ref_fields) {
        ref_column_indexes.push_back(ref_schema.find_column(c));
    }

	auto table_path = db_dir / current_dbname / table_name;
    auto index_path = table_path / (table_name + "_" + fk_name + ".index");
    if (index_handler->createIndex(index_path.c_str(), fields.size()))
        throw DBException("Create file failed");
	
	auto ref_index_path = db_dir / current_dbname / ref_table_name / (ref_table_name + "_pk.index");

    RecordType record_type = schema.record_type();
    for (auto i = record_handler->begin(); !i.isEnd(); ++i) {
		index_handler->openIndex(ref_index_path.c_str(), ref_fields.size());

        auto record = *i;
        auto values = to_value_list(record, schema);
        record.release(record_type);
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
        auto it = index_handler->find(ints.data());
        if (it.isEnd()) {
			FileSystem::save();
			fs::remove(index_path);
            stringstream ss;
            copy(ints.begin(), ints.end(), ostream_iterator<int>(ss, ", "));
            throw DBException(fmt("Field (%s) in current table is not found in ref table", ss.str().c_str()));
        }

		index_handler->openIndex(index_path.c_str(), fields.size());
		index_handler->ins(ints.data(), i.toInt());
    }
	FileSystem::save();

	FK fk;
	fk.name = fk_name;
	fk.fks = fields;
	fk.ref_table = ref_table_name;
	fk.ref_fks = ref_fields;
	schema.fks.push_back(fk);
	bool suc = schema.write(current_dbname);
	if(!suc) throw DBException("Write schema failed");

    return "Added";
}
