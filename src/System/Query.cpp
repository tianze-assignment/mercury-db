#include <vector>
#include <string>
#include <regex>
#include "fort.hpp"

#include "Query.h"
#include "DBManager.h"

using namespace std;

int Condition::cmpVarchar(const Value& a, const Value& b) {
    for (int i = 0; i < a.bytes.size() && i < b.bytes.size(); ++i)
        if (a.bytes[i] != b.bytes[i]) return a.bytes[i] < b.bytes[i] ? -1 : 1;
    return a.bytes.size() < b.bytes.size() ? -1 : a.bytes.size() == b.bytes.size() ? 0 : 1;
}

int Condition::cmpIntOrFloat(const Value& a, const Value& b) {
    if (a.type == INT) {
        int val_a = *((int*)a.bytes.data());
        if (b.type == INT) {
            int val_b = *((int*)b.bytes.data());
            return val_a < val_b ? -1 : val_a == val_b ? 0 : 1;
        }
        else {
            float val_b = *((float*)b.bytes.data());
            return val_a < val_b ? -1 : val_a == val_b ? 0 : 1;
        }
    }
    else {
        float val_a = *((float*)a.bytes.data());
        if (b.type == INT) {
            int val_b = *((int*)b.bytes.data());
            return val_a < val_b ? -1 : val_a == val_b ? 0 : 1;
        }
        else {
            float val_b = *((float*)b.bytes.data());
            return val_a < val_b ? -1 : val_a == val_b ? 0 : 1;
        }
    }
}

bool Condition::cmpLike(const Value&a, const Value&b) {
    string s(a.bytes.begin(),a.bytes.end());
    string m(b.bytes.begin(),b.bytes.end());
    regex sp("[{}()\\[\\].+*?^$\\\\|]");
    string p;
    for (int i = 0; i < m.size(); ++i) {
        if (m[i] == '%') p += ".*";
        else if (m[i] == '_') p += '.';
        else {
            if (m[i] == '\\' && i+1 < m.size()) ++i;
            if (regex_match(string(m.begin()+i, m.begin()+i+1), sp)) p += "\\";
            p += m[i];
        }
    }
    return regex_match(s, regex(p));
}

bool Condition::cmp(const Value& a, const Value& b, CMP_OP op) {
    if (op == IS) return (a.type == NULL_TYPE) ^ (b.type != NULL_TYPE);
    if (a.type == NULL_TYPE || b.type == NULL_TYPE) return false;
    if ((a.type == VARCHAR) ^ (b.type == VARCHAR)) throw DBException("Values to compare should have a same type");
    if (op == LIKE) return cmpLike(a, b);
    int cmp = (a.type == VARCHAR ? cmpVarchar : cmpIntOrFloat)(a, b);
    switch (op)
    {
    case EQUAL:
        return cmp == 0;
    case LESS:
        return cmp < 0;
    case LESS_EQUAL:
        return cmp <= 0;
    case GREATER:
        return cmp > 0;
    case GREATER_EQUAL:
        return cmp >= 0;
    case NOT_EQUAL:
        return cmp != 0;
    default:
        throw DBException("Invalid CMP_OP");
    }
}

string Query::to_str() {
    if (value_lists.empty()) return "";
    fort::char_table table;
    table << fort::header;
    for (auto col: columns) table << col.first + "." + col.second;
    table << fort::endr;
    for (auto val_list: value_lists) {
        for (auto val: val_list) {
            if (val.type == NULL_TYPE) table << "";
            else if (val.type == VARCHAR) table << string(val.bytes.begin(), val.bytes.end());
            else if (val.type == INT) table << *((int*)val.bytes.data());
            else table << *((float*)val.bytes.data());
        }
        table << fort::endr;
    }
    return table.to_string();
}

Value Query::to_value() {
    if (columns.size() != 1 || value_lists.size() != 1)
        throw DBException("There should be exactly one value in set");
    return value_lists[0][0];
}