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
            record.varchar_data[varchar_count++] = string(value.bytes.begin(), value.bytes.end());
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
                auto& s = record.varchar_data[varchar_count];
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

void DBManager::check_ins_pk(const Schema& schema, const vector<Value>& value_list) {
    if (!schema.pk.pks.empty()) {
        vector<int> pk_values;
        for (auto pk: schema.pk.pks) {
            int pki = schema.find_column(pk);
            if (value_list[pki].type == NULL_TYPE) throw DBException("Primary key should not be NULL");
            pk_values.push_back(*((int*)value_list[pki].bytes.data()));  
        }
        auto index_path = db_dir / current_dbname / schema.table_name / (schema.table_name + "_pk.index");
        index_handler->openIndex(index_path.c_str(), schema.pk.pks.size());
        if (!index_handler->find(pk_values.data()).isEnd())
            throw DBException("Duplicate primary key");
    }
}

void DBManager::check_ins_fk(const Schema& schema, const vector<Value>& value_list) {
    for (auto fk: schema.fks) {
        vector<int> fk_values;
        bool has_null = false;
        for (auto fk_col: fk.fks) {
            int fki = schema.find_column(fk_col);
            if (value_list[fki].type == NULL_TYPE) {has_null = true; break;}
            fk_values.push_back(*((int*)value_list[fki].bytes.data()));
        }
        if (has_null) continue;
        auto ref_index_path = db_dir / current_dbname / fk.ref_table / (fk.ref_table + "_pk.index");
        index_handler->openIndex(ref_index_path.c_str(), fk.fks.size());
        if (index_handler->find(fk_values.data()).isEnd())
            throw DBException(string("Invalid value for foreign key \"") + fk.name + "\"");
    }
}

vector<pair<string,FK>> DBManager::get_fks_ref(const Schema& schema) {
    vector<pair<string, FK>> fks_ref_current;
    for(auto i : schemas){
        for(auto fk : i.second.fks){
            if(fk.ref_table == schema.table_name && fk.ref_fks == schema.pk.pks){
                fks_ref_current.push_back(pair<string, FK>(i.first, fk));
            }
        }
    }
    return fks_ref_current;
}

vector<int> DBManager::get_pk_values(const Schema& schema, const vector<Value>& value_list) {
    vector<int> pk_values;
    for(auto pk_col : schema.pk.pks) {
        int pk_i = schema.find_column(pk_col);
        pk_values.push_back(*((int*)value_list[pk_i].bytes.data()));
    }
    return pk_values;
}

void DBManager::check_del_pk(const vector<pair<string,FK>>& fks_ref, const vector<int>& pk_values) {
    for(auto &fk_ref : fks_ref) {
        auto index_path = db_dir / current_dbname / fk_ref.first / (fk_ref.first + "_" + fk_ref.second.name + ".index");
        index_handler->openIndex(index_path.c_str(), fk_ref.second.fks.size());
        if(!index_handler->find(pk_values.data()).isEnd())
            throw DBException("The row to be edited is referenced by table " + fk_ref.first);
    }
}

string DBManager::insert(string table_name, vector<vector<Value>> &value_lists){
    check_db();
    Schema& schema = get_schema(table_name);
    clock_t start = clock();
    open_record(schema);
	auto table_path = db_dir / current_dbname / table_name;

    int count = 0;
    vector<string> fails;
    for(auto value_list: value_lists) {
        RecordType _type;
        Record record(_type);
        try {
            record = to_record(value_list, schema); // check format first
            check_ins_pk(schema, value_list);
            check_ins_fk(schema, value_list);
        }
        catch (DBException e) {
            fails.push_back(e.what());
            continue;
        }
        // insert record
        ++count;
        auto index_val = record_handler->ins(record).toInt();
        for (auto index: schema.get_indexes()) {
            vector<int> key_values;
            bool has_null = false;
            for (auto key: index.second) {
                int ki = schema.find_column(key);
                if (value_list[ki].type == NULL_TYPE) {has_null = true; break;}
                key_values.push_back(*((int*)value_list[ki].bytes.data()));  
            }
            if (has_null) continue;
            index_handler->openIndex((table_path / index.first).c_str(), index.second.size());
            index_handler->ins(key_values.data(), index_val);
        }
    }
    double use_time = (double)(clock() - start) / CLOCKS_PER_SEC;
    string result = "Insert " + rows_text(count) + " OK (" + to_string(use_time) + " Sec)";
    if (!fails.empty()) {
        result += "\n" + rows_text(fails.size()) += " failed:";
        for (auto fail: fails) result += "\n    " + fail;
    }
    return result;
}

bool DBManager::check_conditions(const vector<Value>& value_list, const NameMap& column_map, const vector<Condition>& conditions) {
    for (auto cond: conditions) {
        Value a = value_list[column_map.at(cond.a.second)];
        if (cond.op == IN) {
            if (cond.check_in(a)) continue;
            else break;
        }
        Value b;
        if (cond.b_col.second.empty()) b = cond.b_val;
        else b = value_list[column_map.at(cond.b_col.second)];
        if (!Condition::cmp(a, b, cond.op)) return false;
    }
    return true;
}

string DBManager::delete_(string table_name, vector<Condition> conditions) {
    check_db();
    Schema& schema = get_schema(table_name);
    clock_t start = clock();
    open_record(schema);
    // init map and check conditions
    NameMap column_map;
    for (int i = 0; i < schema.columns.size(); ++i)
        column_map[schema.columns[i].name] = i;
    for (auto cond: conditions) {
        check_column(table_name, column_map, cond.a);
        if (!cond.b_col.second.empty()) check_column(table_name, column_map, cond.b_col);
    }
    // find tables whose fk references current table
    auto fks_ref_current = get_fks_ref(schema);
    // delete
    int count = 0;
    vector<string> fails;
    for (auto it = record_handler->begin(); !it.isEnd(); ) {
        auto value_list = to_value_list(*it, schema);
        if (check_conditions(value_list, column_map, conditions)) {
            // fk constraint check
            auto pk_values = get_pk_values(schema, value_list);
            try {
                check_del_pk(fks_ref_current, pk_values);
            }    
            catch (DBException e) {
                fails.push_back(e.what());
                continue;
            }
            // delete from index
            int index_val = it.toInt();
            for(auto index : schema.get_indexes()){
                vector<int> key_values;
                bool has_null = false;
                for(auto key : index.second){
                    int ki = schema.find_column(key);
                    if(value_list[ki].type == NULL_TYPE) {has_null = true; break;}
                    key_values.push_back(*((int*)value_list[ki].bytes.data()));
                }
                if(has_null) continue;
                index_handler->openIndex((db_dir/current_dbname/table_name/index.first).c_str(), index.second.size());
                index_handler->del(key_values.data(), index_val);
            }

            ++count;
            record_handler->del(it++);
        }
        else ++it;
    }
    double use_time = (double)(clock() - start) / CLOCKS_PER_SEC;
    string result = "Delete " + rows_text(count) + " OK (" + to_string(use_time) + " Sec)";
    if (!fails.empty()) {
        result += "\n" + rows_text(fails.size()) += " failed:";
        for (auto fail: fails) result += "\n    " + fail;
    }
    return result;
}

string DBManager::update(string table_name, vector<pair<string,Value>> assignments, vector<Condition> conditions) {
    check_db();
    Schema& schema = get_schema(table_name);
    clock_t start = clock();
    open_record(schema);
    // init map and check assignments and conditions
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
    // find tables whose fk references current table
    auto fks_ref_current = get_fks_ref(schema);
    // update
    int count = 0;
    vector<string> fails;
    for (auto it = record_handler->begin(); !it.isEnd(); ) {
        auto value_list = to_value_list(*it, schema);
        if (check_conditions(value_list, column_map, conditions)) {
            ++count;
            auto old_value_list = value_list;
            auto old_pk_values = get_pk_values(schema, value_list);
            for (auto assignment: assignments)
                value_list[column_map[assignment.first]] = assignment.second;
            
            RecordType _type;
            Record record(_type);
            try {
                // check format first
                record = to_record(value_list, schema);
                // check fk,pk constraint
                auto pk_values = get_pk_values(schema, value_list);
                if (pk_values != old_pk_values) {
                    check_del_pk(fks_ref_current, old_pk_values);
                    check_ins_pk(schema, value_list);
                }
                check_ins_fk(schema, value_list);
            }
            catch (DBException e) {
                fails.push_back(e.what());
                continue;
            }

            // update record
            int old_index_val = it.toInt();
            int index_val = record_handler->upd(it++, record).toInt();

            // update index
            for(auto index : schema.get_indexes()){
                vector<int> key_values, old_key_values;
                bool has_null = false, old_has_null = false;
                for(auto key : index.second){
                    int ki = schema.find_column(key);
                    if (value_list[ki].type == NULL_TYPE) has_null = true;
                    if (old_value_list[ki].type == NULL_TYPE) old_has_null = true;
                    if (!has_null) key_values.push_back(*((int*)value_list[ki].bytes.data()));
                    if (!old_has_null) old_key_values.push_back(*((int*)old_value_list[ki].bytes.data()));
                }
                if (has_null && old_has_null) break;
                index_handler->openIndex((db_dir/current_dbname/table_name/index.first).c_str(), index.second.size());
                if (!has_null && !old_has_null)
                    index_handler->upd(old_key_values.data(), old_index_val, key_values.data(), index_val);
                else if (!old_has_null)
                    index_handler->del(old_key_values.data(), old_index_val);
                else if (!has_null)
                    index_handler->ins(key_values.data(), index_val);
            }
        }
        else ++it;
    }
    double use_time = (double)(clock() - start) / CLOCKS_PER_SEC;
    string result = "Update " + rows_text(count) + " OK (" + to_string(use_time) + " Sec)";
    if (!fails.empty()) {
        result += "\n" + rows_text(fails.size()) += " failed:";
        for (auto fail: fails) result += "\n    " + fail;
    }
    return result;
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

Query DBManager::select(vector<QueryCol> cols, vector<string> tables, vector<Condition> conditions,
        Aggregator aggregator, int limit, int offset) {
    check_db();
    Query query;
    query.aggregator = aggregator;
    // init maps and queryColomns
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
    // search
    // -- init its
    vector<RecordHandler::Iterator> its;
    vector<bool> ifound;
    vector<string> inames;
    vector<int> isizes;
    vector<IndexHandler::Iterator> iits, ibegin, iend;
    for (auto schema: schemas) {
        open_record(schema);
        its.push_back(record_handler->begin());
        if (its.back().isEnd()) return query;
        bool found = false;
        string iname;
        int isize;
        auto it = find_index(schema, conditions, found, iname, isize);
        if (found && it.first == it.second) return query;
        ifound.push_back(found);
        inames.push_back(schema.table_name + "/" + iname);
        isizes.push_back(isize);
        iits.push_back(it.first);
        ibegin.push_back(it.first);
        iend.push_back(it.second);
    }
    // -- loop
    while (limit == -1 || query.value_lists.size() < limit) {
        int i;
        // get values
        vector<vector<Value>> value_lists;
        for (i = 0; i < its.size(); ++i) {
            open_record(schemas[i]);
            if (ifound[i]) {
                index_handler->openIndex((db_dir / current_dbname / inames[i]).c_str(), isizes[i]);
                auto it = RecordHandler::Iterator(record_handler, *iits[i]);
                value_lists.push_back(to_value_list(*it, schemas[i]));
            }
            else value_lists.push_back(to_value_list(*its[i], schemas[i]));
        }
        // check conditions
        for (i = 0; i < conditions.size(); ++i) {
            Condition& cond = conditions[i];
            Value a = get_value(value_lists, table_map, column_maps, cond.a);
            if (cond.op == IN) {
                if (cond.check_in(a)) continue;
                else break;
            }
            Value b;
            if (cond.b_col.second.empty()) b = cond.b_val;
            else b = get_value(value_lists, table_map, column_maps, cond.b_col);
            if (!Condition::cmp(a, b, cond.op)) break;
        }
        // conditions ok
        if (i == conditions.size()) {
            vector<Value> value_list;
            for (auto col: query.columns) value_list.push_back(get_value(value_lists, table_map, column_maps, col));
            if (!offset) query += value_list;
            else --offset;
        }
        // ++its
        for (i=its.size()-1; i >= 0 ; --i) {
            if (ifound[i]) {
                index_handler->openIndex((db_dir / current_dbname / inames[i]).c_str(), isizes[i]);
                if (++iits[i] != iend[i]) break;
                iits[i] = ibegin[i];
            }
            else {
                open_record(schemas[i]);
                if (!(++its[i]).isEnd()) break;
                its[i] = record_handler->begin();
            }
        }
        if (i < 0) break;
    }
    return query;
}

pair<IndexHandler::Iterator,IndexHandler::Iterator> DBManager::find_index(
    const Schema& schema, const vector<Condition>& conditions, bool& found, string& iname, int& isize) {
	auto table_path = db_dir / current_dbname / schema.table_name;
    for (auto index: schema.get_indexes()) {
        auto& fileName = index.first;
        auto& cols = index.second;
        vector<int> lv, rv;
        for (auto col: cols) {
            int l = INT32_MIN, r = INT32_MAX;
            for (auto cond: conditions) {
                if (cond.a.first == schema.table_name && cond.a.second == col && cond.b_col.second.empty() &&
                        (cond.op == EQUAL || cond.op == LESS || cond.op == LESS_EQUAL || cond.op == GREATER || cond.op == GREATER_EQUAL)) {
                    int b = *((int*)cond.b_val.bytes.data());
                    if (cond.op == EQUAL) l = max(l, b), r = min(r, b);
                    if (cond.op == LESS) {if (b != INT32_MIN) r = min(r, b-1); else l = INT32_MAX, r = INT32_MIN;}
                    if (cond.op == LESS_EQUAL) r = min(r, b);
                    if (cond.op == GREATER) {if (b != INT32_MAX) l = max(l, b+1); else l = INT32_MAX, r = INT32_MIN;}
                    if (cond.op == GREATER_EQUAL) l = max(l, b);
                }
            }
            if (l > r) {
                found = true;
                iname = fileName;
                isize = cols.size();
                index_handler->openIndex((table_path / fileName).c_str(), cols.size());
                return make_pair(index_handler->end(), index_handler->end());
            }
            lv.push_back(l); rv.push_back(r);
            if (!found) {
                if (l > INT32_MIN || r < INT32_MAX) found = true;
                else break;
            }
        }
        if (found) {
            iname = fileName;
            isize = cols.size();
            index_handler->openIndex((table_path / fileName).c_str(), cols.size());
            auto begin = index_handler->lowerBound(lv.data());
            auto end = index_handler->upperBound(rv.data());
            return make_pair(begin, end);
        }
    }
    return make_pair(index_handler->end(), index_handler->end());
}