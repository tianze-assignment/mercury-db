#include "Schema.h"

#include <fstream>
#include <sstream>

#include "DBManager.h"
#include "fort.hpp"

string Column::type_str() {
    if (type == INT) return "INT";
    if (type == VARCHAR) return "VARCHAR(" + to_string(varchar_len) + ") ";
    if (type == FLOAT) return "FLOAT";
    return "";
}

bool Schema::write(string db_name) {
    ofstream out(string(DB_DIR) + "/" + db_name + "/" + table_name + "/" + table_name + ".schema");
    if (!out) return false;
    // table_name
    out << table_name << " ";
    // columns
    out << columns.size() << " ";
    for (auto &c : columns) {
        out << c.name << " " << c.type << " " << c.varchar_len << " " << c.not_null << " ";
        // default
        out << c.has_default << " ";
        if (c.has_default) {
            if (c.default_value.type == NULL_TYPE)
                out << "1 ";
            else
                out << "0 ";
            out << c.default_value.bytes.size() << " ";
            for (auto &v : c.default_value.bytes) out << int(v) << " ";
        }
    }
    // pk_name
    if (this->pk.name.empty())
        out << "0 ";
    else
        out << "1 ";
    out << this->pk.name << " ";
    // pks
    out << this->pk.pks.size() << " ";
    for (auto &pk : this->pk.pks) out << pk << " ";
    // fks
    out << fks.size() << " ";
    for (auto &fk : fks) {
        // name
        if (fk.name.empty())
            out << "0 ";
        else
            out << "1 ";
        out << fk.name << " ";
        // fks
        out << fk.fks.size() << " ";
        for (auto &fkfk : fk.fks) out << fkfk << " ";
        // ref_table
        out << fk.ref_table << " ";
        // ref_fks
        out << fk.ref_fks.size() << " ";
        for (auto &ref_fk : fk.ref_fks) out << ref_fk << " ";
    }
    // index
    out << this->indexes.size() << " ";
    for (auto index : indexes) {
        out << index.size() << " ";
        for (auto i : index) out << i << " ";
    }
    return true;
}

Schema::Schema() {}

Schema::Schema(string table_name, string db_name) {
    ifstream in(string(DB_DIR) + "/" + db_name + "/" + table_name + "/" + table_name + ".schema");
    // table_name
    in >> this->table_name;
    // columns
    int size;
    in >> size;
    for (int i = 0; i < size; i++) {
        Column column;
        int type;
        in >> column.name >> type;
        column.type = static_cast<Type>(type);
        in >> column.varchar_len >> column.not_null >> column.has_default;
        if (column.has_default) {
            bool is_null;
            in >> is_null;
            if (is_null)
                column.default_value.type = NULL_TYPE;
            else
                column.default_value.type = column.type;
            int default_value_size;
            in >> default_value_size;
            for (int j = 0; j < default_value_size; j++) {
                int v;
                in >> v;
                column.default_value.bytes.push_back(uint8_t(v));
            }
        }
        this->columns.push_back(column);
    }
    // pk_name
    int exist;
    in >> exist;
    if (exist) in >> this->pk.name;
    // pks
    in >> size;
    for (int i = 0; i < size; i++) {
        string pk;
        in >> pk;
        this->pk.pks.push_back(pk);
    }
    // fks
    in >> size;
    for (int i = 0; i < size; i++) {
        FK fk;
        // name
        in >> exist;
        if (exist) in >> fk.name;
        // fks
        int sub_size;
        in >> sub_size;
        for (int j = 0; j < sub_size; j++) {
            string sub_fk;
            in >> sub_fk;
            fk.fks.push_back(sub_fk);
        }
        // ref_table
        in >> fk.ref_table;
        // ref_fks
        in >> sub_size;
        for (int j = 0; j < sub_size; j++) {
            string sub_ref_fk;
            in >> sub_ref_fk;
            fk.ref_fks.push_back(sub_ref_fk);
        }
        this->fks.push_back(fk);
    }
    // index
    in >> size;
    for (int i = 0; i < size; i++) {
        this->indexes.push_back(vector<string>());
        int sub_size;
        in >> sub_size;
        for (int j = 0; j < sub_size; j++) {
            string sub_index;
            in >> sub_index;
            this->indexes[i].push_back(sub_index);
        }
    }
}

string Schema::to_str() {
    stringstream ss;

    fort::char_table table;
    table << fort::header
          << "Field"
          << "Type"
          << "Not NULL"
          << "Default" << fort::endr;
    for (auto &col : this->columns) {
        table << col.name << col.type_str()
              << (col.not_null ? "YES" : "NO");
        if (col.has_default) {  // if has default
            if (col.default_value.type == INT) table << *((int *)(&col.default_value.bytes[0]));
            if (col.default_value.type == FLOAT) table << *((float *)(&col.default_value.bytes[0]));
            if (col.default_value.type == VARCHAR) table << "'" + string(col.default_value.bytes.begin(), col.default_value.bytes.end()) + "'";
            if (col.default_value.type == NULL_TYPE) table << "NULL";
        } else {
            table << "NO";
        }
        table << fort::endr;
    }
    ss << table.to_string() << "\n";
    // pk
    if (!this->pk.pks.empty()) {
        ss << "PRIMARY KEY ";
        if (!this->pk.name.empty()) ss << this->pk.name << " ";
        ss << "(";
        for (auto k : this->pk.pks) ss << k << ", ";
        ss << "),\n";
    }
    // fk
    for (auto fk : this->fks) {
        ss << "FOREIGN KEY ";
        if (!fk.name.empty()) ss << fk.name << " ";
        ss << "(";
        for (auto k : fk.fks) ss << k << ", ";
        ss << ") REFERENCES " << fk.ref_table << " (";
        for (auto k : fk.ref_fks) ss << k << ", ";
        ss << "),\n";
    }
    // index
    for(auto index : this->indexes){
        ss << "INDEX (";
        for(auto i : index) ss << i << ", ";
        ss << "),\n";
    }
    
    return ss.str();
}

int Schema::find_column(string &name) const {
    int i = 0;
    for (; i < this->columns.size(); i++) {
        if (this->columns[i].name == name) break;
    }
    return i;
}

int Schema::find_fk_by_name(string &name){
    int i = 0;
    for(; i < this->fks.size(); i++){
        if(this->fks[i].name == name) break;
    }
    return i;
}

RecordType Schema::record_type() const {
    RecordType res;
    for (auto column : columns) {
        if (column.type == VARCHAR)
            ++res.num_varchar;
        else
            ++res.num_int;
    }
    return res;
}

vector<pair<string,vector<string>>> Schema::get_indexes() {
    vector<pair<string,vector<string>>> res;
    if (!pk.pks.empty()) res.push_back(make_pair(table_name + "_pk.index", pk.pks));
    for (auto fk: fks) res.push_back(make_pair(table_name + "_" + fk.name + ".index", fk.fks));
    for (int i = 0; i < indexes.size(); ++i)
        res.push_back(make_pair(table_name + to_string(i) + ".index", indexes[i]));
    return res;
}