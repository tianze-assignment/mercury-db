#pragma once

#include <string>
#include <vector>

#include "Record.h"

using namespace std;

enum Type { INT,
            VARCHAR,
            FLOAT,
            NULL_TYPE };

struct Value {
    Type type;
    vector<uint8_t> bytes;
    Value() {type = NULL_TYPE;}
    Value(int x) {type = INT; bytes = vector<uint8_t>((uint8_t*)(&x), (uint8_t*)(&x) + 4); }
    Value(float x) {type = FLOAT; bytes = vector<uint8_t>((uint8_t*)(&x), (uint8_t*)(&x) + 4); }
    Value(string x) {type = INT; bytes = vector<uint8_t>(x.begin(), x.end());}
    int toInt() const {return *((int*)bytes.data());}
    float toFloat() const {return *((float*)bytes.data());}
    string toString() const {return string(bytes.begin(), bytes.end());}
};

struct Column {
    string name;
    Type type;
    int varchar_len;
    bool not_null;
    // bool default_null;
    // vector<uint8_t> default_value;  // zero length if no default
	bool has_default;
	Value default_value;
	
	string type_str();
};

struct PK {
    string name;
    vector<string> pks;
};

struct FK {
    string name;
    vector<string> fks;
    string ref_table;
    vector<string> ref_fks;
};

class Schema {
   public:
    string table_name;
    vector<Column> columns;
    PK pk;
    vector<FK> fks;
	vector<vector<string>> indexes;

    Schema();
    Schema(string table_name, string db_name);
    bool write(string db_name);
    string to_str();
    int find_column(string &name) const;
    int find_fk_by_name(string &name);
    RecordType record_type() const;
    vector<pair<string,vector<string>>> get_indexes() const;
};
