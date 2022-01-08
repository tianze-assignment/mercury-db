#pragma once

#include <string>
#include <vector>

using namespace std;

enum Type { INT,
            VARCHAR,
            FLOAT };

struct Column {
    string name;
    Type type;
    int varchar_len;
    bool not_null;
    bool default_null;
    vector<uint8_t> default_value;  // zero length if no default
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

    Schema();
    Schema(string table_name, string db_name);
    bool write(string db_name);
    string to_str();
	int find_column(string &name);
};
