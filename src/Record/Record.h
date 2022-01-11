#pragma once

#include <iostream>
#include <vector>

using namespace std;

struct RecordType{
    int num_int, num_varchar;
    RecordType(int num_int, int num_varchar)
        :num_int(num_int), num_varchar(num_varchar){}
    RecordType():RecordType(0,0){}
};

struct Record{
    vector<bool> int_null, varchar_null;
    vector<int> int_data;
    vector<string> varchar_data;
    Record(const RecordType& type) {
        int_null = vector<bool>(type.num_int);
        int_data = vector<int>(type.num_int);
        varchar_null = vector<bool>(type.num_varchar);
        varchar_data = vector<string>(type.num_varchar);
    }
    void output(std::ostream& os, const RecordType& type) {
        for (int i = 0; i < type.num_int; ++i)
            int_null[i] ? (os << "NULL ") : (os << int_data[i] << " ");
        for (int i = 0; i < type.num_varchar; ++i)
            varchar_null[i] ? (os << "NULL ") : (os << "\"" << varchar_data[i] << "\" ");
        os << std::endl;
    }
};