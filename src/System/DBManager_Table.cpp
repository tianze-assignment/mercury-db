#include <vector>
#include <map>
#include <ctime>

#include "DBManager.h"
#include "Query.h"

using namespace std;

string DBManager::file_name(const Schema& schema) {
    return string(DB_DIR) + "/" + current_dbname + "/" + schema.table_name + "/" + schema.table_name;
}

void DBManager::open_record(const Schema& schema) {
    record_handler->openFile((file_name(schema) + ".data").data(), schema.record_type());
}

string DBManager::rows_text(int row) {
    return to_string(row) + " row" + (row > 1 ? "s" : "");
}

Schema& DBManager::get_schema(const string& table_name) {
	if(schemas.find(table_name) == schemas.end()) throw DBException("Table does not exist");
    return schemas[table_name];
}

Record DBManager::to_record(vector<Value>& value_list, const Schema& schema) {
    if (value_list.size() != schema.columns.size()) throw DBException("Invalid number of values");
    Record record(schema.record_type());
    int int_count = 0, varchar_count = 0;
    for (int i = 0; i < schema.columns.size(); ++i) {
        auto& column = schema.columns[i];
        auto& value = value_list[i];
        if (value.type == NULL_TYPE) {
            if (column.not_null) throw DBException((string)"Column \"" + column.name + "\" should not be NULL");
            if (column.type == VARCHAR) record.varchar_null[varchar_count++] = true;
            else record.int_null[int_count++] = true; 
        }
        else if (value.type != column.type && !(column.type == FLOAT && value.type == INT))
            throw DBException((string)"Invalid value type of Column \"" + column.name + "\"");
        else if (value.type == VARCHAR) {
            record.varchar_null[varchar_count] = false;
            if (value.bytes.size() > column.varchar_len)
                throw DBException((string)"Varchar \"" + column.name + "\" too long");
            value.bytes.push_back('\0');
            record.varchar_data[varchar_count++] = (char *)value.bytes.data();
        }
        else {
            record.int_null[int_count] = false;
            if (column.type == FLOAT && value.type == INT) {
                float v = *((int*)value.bytes.data());
                record.int_data[int_count++] = *((int*)&v);
            }
            else record.int_data[int_count++] = *((int*)value.bytes.data());
        }
    }
    return record;
}

vector<Value> DBManager::to_value_list(const Record& record, const Schema& schema) {
    vector<Value> value_list;
    int int_count = 0, varchar_count = 0;
    for (auto column: schema.columns) {
        Value v;
        if (column.type == VARCHAR) {
            if (record.varchar_null[varchar_count]) v.type = NULL_TYPE;
            else {
                v.type = VARCHAR;
                string s(record.varchar_data[varchar_count]);
                v.bytes = vector<uint8_t>(s.begin(), s.end());
            }
            ++varchar_count;
        }
        else {
            if (record.int_null[int_count]) v.type = NULL_TYPE;
            else {
                v.type = column.type;
                uint8_t* data = (uint8_t*)(&record.int_data[int_count]);
                v.bytes = vector<uint8_t>(data, data+4);
            }
            ++int_count;
        }
        value_list.push_back(v);
    }
    return value_list;
}

string DBManager::insert(string table_name, vector<vector<Value>> &value_lists){
    check_db();
    Schema& schema = get_schema(table_name);
    clock_t start = clock();
    open_record(schema);
    for(auto value_list: value_lists) record_handler->ins(to_record(value_list, schema));
    double use_time = (double)(clock() - start) / CLOCKS_PER_SEC;
    return "Insert " + rows_text(value_lists.size()) + " OK (" + to_string(use_time) + " Sec)";
}

string DBManager::delete_(string table_name, vector<Condition> conditions) {
    check_db();
    Schema& schema = get_schema(table_name);
    clock_t start = clock();
    open_record(schema);
    NameMap column_map;
    for (int i = 0; i < schema.columns.size(); ++i)
        column_map[schema.columns[i].name] = i;
    for (auto cond: conditions) {
        check_column(table_name, column_map, cond.a);
        if (!cond.b_col.second.empty()) check_column(table_name, column_map, cond.b_col);
    }
    int count = 0;
    for (auto it = record_handler->begin(); !it.isEnd(); ) {
        auto value_list = to_value_list(*it, schema);
        int i;
        for (i = 0; i < conditions.size(); ++i) {
            Condition& cond = conditions[i];
            Value a = value_list[column_map[cond.a.second]];
            Value b;
            if (cond.b_col.second.empty()) b = cond.b_val;
            else b = value_list[column_map[cond.b_col.second]];
            if (!Condition::cmp(a, b, cond.op)) break;
        }
        if (i == conditions.size()) {
            ++count;
            record_handler->del(it++);
        }
        else ++it;
    }
    double use_time = (double)(clock() - start) / CLOCKS_PER_SEC;
    return "Delete " + rows_text(count) + " OK (" + to_string(use_time) + " Sec)";
}

string DBManager::update(string table_name, vector<pair<string,Value>> assignments, vector<Condition> conditions) {
    check_db();
    Schema& schema = get_schema(table_name);
    clock_t start = clock();
    open_record(schema);
    NameMap column_map;
    for (int i = 0; i < schema.columns.size(); ++i)
        column_map[schema.columns[i].name] = i;
    for (auto assignment: assignments)
        if (column_map.find(assignment.first) == column_map.end())
            throw DBException((string)"Column \"" + assignment.first + "\" does not exist");
    for (auto cond: conditions) {
        check_column(table_name, column_map, cond.a);
        if (!cond.b_col.second.empty()) check_column(table_name, column_map, cond.b_col);
    }
    int count = 0;
    for (auto it = record_handler->begin(); !it.isEnd(); ) {
        auto value_list = to_value_list(*it, schema);
        int i;
        for (i = 0; i < conditions.size(); ++i) {
            Condition& cond = conditions[i];
            Value a = value_list[column_map[cond.a.second]];
            Value b;
            if (cond.b_col.second.empty()) b = cond.b_val;
            else b = value_list[column_map[cond.b_col.second]];
            if (!Condition::cmp(a, b, cond.op)) break;
        }
        if (i == conditions.size()) {
            ++count;
            for (auto assignment: assignments)
                value_list[column_map[assignment.first]] = assignment.second;
            record_handler->upd(it++, to_record(value_list, schema));
        }
        else ++it;
    }
    double use_time = (double)(clock() - start) / CLOCKS_PER_SEC;
    return "Update " + rows_text(count) + " OK (" + to_string(use_time) + " Sec)";
}

void DBManager::check_column(const string& table_name, const NameMap& column_map, const QueryCol& col) {
    if (col.first != table_name)
        throw DBException((string)"Table \"" + col.first + "\" has not been selected");
    if (column_map.find(col.second) == column_map.end())
        throw DBException((string)"Column \"" + col.first + "." + col.second + "\" does not exist");
}

void DBManager::check_column(const NameMap& table_map, const vector<NameMap>& column_maps, const QueryCol& col) {
    if (table_map.find(col.first) == table_map.end())
        throw DBException((string)"Table \"" + col.first + "\" has not been selected");
    int table = table_map.at(col.first);
    if (column_maps[table].find(col.second) == column_maps[table].end())
        throw DBException((string)"Column \"" + col.first + "." + col.second + "\" does not exist");
}

Value DBManager::get_value(const vector<vector<Value>>& value_lists,
        const NameMap& table_map, const vector<NameMap>& column_maps, const QueryCol& col) {
    int table = table_map.at(col.first);
    int column = column_maps[table].at(col.second);
    return value_lists[table][column];
}

Query DBManager::select(vector<QueryCol> cols, vector<string> tables, vector<Condition> conditions) {
    check_db();
    Query query;
    vector<Schema> schemas;
    NameMap table_map;
    vector<NameMap> column_maps;
    for (int i = 0; i < tables.size(); ++i) {
        schemas.push_back(get_schema(tables[i]));
        if (table_map.find(tables[i]) != table_map.end()) throw DBException("Duplicate table \"" + tables[i] + "\"");
        table_map[tables[i]] = i;
        column_maps.push_back(NameMap());
        for (int j = 0; j < schemas[i].columns.size(); ++j)
            column_maps[i][schemas[i].columns[j].name] = j;
    }
    if (cols.empty()) {
        for (auto schema: schemas) for (auto column: schema.columns)
            query.columns.push_back(make_pair(schema.table_name, column.name));
    }
    else {
        for (auto col: cols) {
            check_column(table_map, column_maps, col);
            query.columns.push_back(col);
        }
    }
    for (auto cond: conditions) {
        check_column(table_map, column_maps, cond.a);
        if (!cond.b_col.second.empty()) check_column(table_map, column_maps, cond.b_col);
    }
    vector<RecordHandler::Iterator> its;
    for (auto schema: schemas) {
        open_record(schema);
        its.push_back(record_handler->begin());
        if (its.back().isEnd()) return query;
    }
    while (true) {
        int i;
        vector<vector<Value>> value_lists;
        for (i = 0; i < its.size(); ++i) {
            open_record(schemas[i]);
            value_lists.push_back(to_value_list(*its[i], schemas[i]));
        }
        for (i = 0; i < conditions.size(); ++i) {
            Condition& cond = conditions[i];
            Value a = get_value(value_lists, table_map, column_maps, cond.a);
            Value b;
            if (cond.b_col.second.empty()) b = cond.b_val;
            else b = get_value(value_lists, table_map, column_maps, cond.b_col);
            if (!Condition::cmp(a, b, cond.op)) break;
        }
        if (i == conditions.size()) {
            vector<Value> value_list;
            for (auto col: query.columns) value_list.push_back(get_value(value_lists, table_map, column_maps, col));
            query.value_lists.push_back(value_list);
        }
        for (i=its.size()-1; i >= 0 ; --i) {
            open_record(schemas[i]);
            if (!(++its[i]).isEnd()) break;
            its[i] = record_handler->begin();
        }
        if (i < 0) break;
    }
    return query;
}