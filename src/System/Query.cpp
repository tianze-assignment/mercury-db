#include <vector>
#include <string>
#include "fort.hpp"

#include "Query.h"

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

bool Condition::cmp(const Value& a, const Value& b, CMP_OP op) {
    switch (op)
    {
    case EQUAL:
        if (a.type == NULL_TYPE) return b.type == NULL_TYPE;
        if (b.type == NULL_TYPE) return false;
        if (a.type == VARCHAR) {
            if (b.type != VARCHAR) return false;
            return cmpVarchar(a, b) == 0;
        }
        if (b.type == VARCHAR) return false;
        return cmpIntOrFloat(a, b) == 0;

    case LESS:
        if (a.type == NULL_TYPE || b.type == NULL_TYPE) return false;
        if (a.type == VARCHAR) {
            if (b.type != VARCHAR) return false;
            return cmpVarchar(a, b) < 0;
        }
        if (b.type == VARCHAR) return false;
        return cmpIntOrFloat(a, b) < 0;

    case LESS_EQUAL:
        if (a.type == NULL_TYPE) return b.type == NULL_TYPE;
        if (b.type == NULL_TYPE) return false;
        if (a.type == VARCHAR) {
            if (b.type != VARCHAR) return false;
            return cmpVarchar(a, b) <= 0;
        }
        if (b.type == VARCHAR) return false;
        return cmpIntOrFloat(a, b) <= 0;

    case GREATER:
        if (a.type == NULL_TYPE || b.type == NULL_TYPE) return false;
        if (a.type == VARCHAR) {
            if (b.type != VARCHAR) return false;
            return cmpVarchar(a, b) > 0;
        }
        if (b.type == VARCHAR) return false;
        return cmpIntOrFloat(a, b) > 0;

    case GREATER_EQUAL:
        if (a.type == NULL_TYPE) return b.type == NULL_TYPE;
        if (b.type == NULL_TYPE) return false;
        if (a.type == VARCHAR) {
            if (b.type != VARCHAR) return false;
            return cmpVarchar(a, b) >= 0;
        }
        if (b.type == VARCHAR) return false;
        return cmpIntOrFloat(a, b) >= 0;
    
    case NOT_EQUAL:
        return !cmp(a, b, EQUAL);

    default:
        return false;
    }

}

string Query::to_str() {
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