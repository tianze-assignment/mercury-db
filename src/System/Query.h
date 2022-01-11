#pragma once

#include <vector>
#include <string>
#include "fort.hpp"

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
    CNT,
    AVG,
    MAX,
    MIN,
    SUM,
    CNT_
};

struct Aggregator{
    vector<Aggregator_OP> ops;
    QueryCol group_by;
};

class Query {
private:
    void new_empty_agg();
    void solve_agg(int index, const vector<Value>& value_list);
    void output(fort::char_table& table, const Value& value);
public:
    vector<QueryCol> columns;
    Aggregator aggregator;
    vector<vector<Value>> value_lists;
    string to_str();
    Value to_value();
    void operator+=(const vector<Value>& value_list);
};
