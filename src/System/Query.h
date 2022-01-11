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
    NOT_EQUAL,
    IS,
    IN,
    LIKE
};

typedef pair<string,string> QueryCol;

struct Condition{
    QueryCol a,b_col;
    Value b_val;
    vector<vector<Value>> b_value_lists;
    CMP_OP op;
    static int cmpVarchar(const Value& a, const Value& b);
    static int cmpIntOrFloat(const Value& a, const Value& b);
    static bool cmpLike(const Value& a, const Value& b);
    static bool cmp(const Value& a, const Value& b, CMP_OP op);
    bool check_in(const Value& a);
};

enum Aggregator_OP {
    Count,
    Average,
    Max,
    Min,
    Sum
};

struct Aggregator{
    vector<Aggregator_OP> ops;
};

struct Query {
public:
    vector<QueryCol> columns;
    Aggregator aggregator;
    vector<vector<Value>> value_lists;
    string to_str();
    Value to_value();
};
