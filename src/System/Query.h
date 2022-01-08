#pragma once

#include <vector>
#include <string>

#include "Schema.h"

using namespace std;

enum CMP_OP{
    EQUAL,
    LESS,
    LESS_EQUAL,
    GREATER,
    GREATER_EQUAL,
    NOT_EQUAL
};

typedef pair<string,string> QueryCol;

struct Condition{
    QueryCol a,b_col;
    Value b_val;
    CMP_OP op;
};

class Query {
public:
    vector<QueryCol> columns;
    vector<vector<Value>> value_lists;
    string to_str();
};