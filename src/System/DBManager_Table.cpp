#include <vector>
#include <map>

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

Record DBManager::to_record(const vector<Value>& value_list, const Schema& schema) {
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
            record.varchar_data[varchar_count++] = string(value.bytes.begin(), value.bytes.end()).data();
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
    RecordType type = schema.record_type();
    open_record(schema);
    for(auto value_list: value_lists) record_handler->ins(to_record(value_list, schema));
    return "Insert " + rows_text(value_lists.size()) + " OK";
}

void DBManager::check_column(const NameMap& table_map, const vector<NameMap>& column_maps, const QueryCol& col) {
    if (table_map.find(col.first) == table_map.end())
        throw DBException((string)"Table \"" + col.first + "\" does not exist");
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