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

bool Condition::check_in(const Value& a) {
    for (auto value_list: b_value_lists)
        for (auto value: value_list) if (cmp(a, value, EQUAL)) return true;
    return false;
}

void Query::output(fort::char_table& table, const Value& val) {
    if (val.type == NULL_TYPE) table << "";
    else if (val.type == VARCHAR) table << string(val.bytes.begin(), val.bytes.end());
    else if (val.type == INT) table << *((int*)val.bytes.data());
    else table << *((float*)val.bytes.data());
}

string Query::to_str() {
    if (value_lists.empty()) return "";
    fort::char_table table;
    table << fort::header;
    if (!aggregator.ops.empty()) {
        if (!aggregator.group_by.second.empty())
            table << aggregator.group_by.first + "." + aggregator.group_by.second;
        int i = 0;
        for (auto op: aggregator.ops) {
            if (op == CNT_) {
                table << "COUNT(*)";
                continue;
            }
            string s;
            if (op == CNT) s = "COUNT";
            if (op == AVG) s = "AVG";
            if (op == MAX) s = "MAX";
            if (op == MIN) s = "MIN";
            if (op == SUM) s = "SUM";
            table << s + "(" + columns[i].first + "." + columns[i].second + ")";
            ++i;
        }
    }
    else for (auto col: columns) table << col.first + "." + col.second;
    table << fort::endr;
    for (auto val_list: value_lists) {
        if (!aggregator.ops.empty()) {
            if (!aggregator.group_by.second.empty()) output(table, val_list.back());
            int i = 0;
            for (auto op: aggregator.ops) {
                if (op == AVG) {
                    Value v;
                    if (val_list[i+1].toInt() > 0) v = val_list[i].toFloat() / (float)val_list[i+1].toInt();
                    output(table, v);
                    i += 2;
                }
                else output(table, val_list[i++]);
            }
        }
        else for (auto val: val_list) output(table, val);
        table << fort::endr;
    }
    return table.to_string();
}

Value Query::to_value() {
    if (columns.size() != 1 || value_lists.size() != 1)
        throw DBException("There should be exactly one value in set");
    return value_lists[0][0];
}

void Query::new_empty_agg() {
    value_lists.push_back(vector<Value>());
    for (auto op: aggregator.ops) {
        Value v;
        if (op == CNT || op == CNT_) v = 0;
        if (op == AVG) value_lists.back().push_back((float)0), v = 0;
        value_lists.back().push_back(v);
    }
}

void Query::solve_agg(int index, const vector<Value>& value_list) {
    auto& agg = value_lists[index];
    int agg_i = 0, val_i = 0;
    for (auto op: aggregator.ops) {
        if (op == CNT_) {
            agg[agg_i] = agg[agg_i].toInt() + 1;
            ++agg_i;
            continue;
        }
        auto& val = value_list[val_i++];
        if (val.type == NULL_TYPE) continue;
        if (op == CNT) agg[agg_i] = agg[agg_i].toInt() + 1;
        if (op == AVG) {
            if (val.type == VARCHAR) throw DBException("Average on VARCHAR is invalid");
            agg[agg_i] = agg[agg_i].toFloat() + (val.type == INT ? (float)val.toInt(): val.toFloat());
            ++agg_i;
            agg[agg_i] = agg[agg_i].toInt() + 1;
        }
        if (op == MAX) {
            if (agg[agg_i].type == NULL_TYPE || Condition::cmp(val, agg[agg_i], GREATER))
                agg[agg_i] = val;
        }
        if (op == MIN) {
            if (agg[agg_i].type == NULL_TYPE || Condition::cmp(val, agg[agg_i], LESS))
                agg[agg_i] = val;
        }
        if (op == SUM) {
            if (val.type == VARCHAR) throw DBException("Sum on VARCHAR is invalid");
            if (agg[agg_i].type == NULL_TYPE) agg[agg_i] = val;
            else if (val.type == INT) agg[agg_i] = agg[agg_i].toInt() + val.toInt();
            else agg[agg_i] = agg[agg_i].toFloat() + val.toFloat();
        }
        ++agg_i;
    }
}

void Query::operator+=(const vector<Value>& value_list) {
    if (aggregator.ops.empty()) {
        value_lists.push_back(value_list);
        return;
    }
    if (aggregator.group_by.second.empty()) {
        if (value_lists.empty()) new_empty_agg();
        solve_agg(0, value_list);
    }
    else {
        bool found = false;
        for (int i = 0; i < value_lists.size(); ++i)
            if (Condition::cmp(value_lists[i].back(), value_list.back(), EQUAL)) {
                found = true;
                solve_agg(i, value_list);
                break;
            }
        if (!found && value_list.back().type != NULL_TYPE) {
            new_empty_agg();
            value_lists.back().push_back(value_list.back());
            solve_agg(value_lists.size()-1, value_list);
        }
    }
}
