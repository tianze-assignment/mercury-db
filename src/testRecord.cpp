#include <iostream>
#include "RecordHandler.h"
using namespace std;
RecordHandler handler;
RecordType type(3,2);
void output() {
    cout << "-----------" << endl;
    for (auto it = handler.begin(); !it.isEnd(); ++it) (*it).output(cout, type);
    cout << "-----------" << endl;
}
int main() {
    handler.openFile("1.data", type);
    /*
    Record record(type);
    record.int_null[0] = record.int_null[1] = false;
    record.int_null[2] = true;
    record.int_data[0] = 1;
    record.int_data[1] = 2;
    record.varchar_null[0] = true;
    record.varchar_null[1] = false;
    record.varchar_data[1] = "hello";
    for (int i = 0 ; i < 10; ++i)
    {
        record.int_data[1] = i +2;
        handler.ins(record);
    }
    
    handler.upd(handler.begin(), record);
    //handler.del(handler.begin());
    */
    output();
}