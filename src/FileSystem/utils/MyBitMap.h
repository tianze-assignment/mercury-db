#pragma once

#include<cstring>
#include<string>

class MyBitMap{
    uint32_t* data;
    int n;
    static char h[61];
	static int _hash(uint i) {return i % 61;}
    static uint _lowbit(uint x) {return x&-x;}
public:
    MyBitMap(int n, int k): n(n) {
        data = new uint32_t[n>>5];
        memset(data, k ? 0xFF: 0, sizeof(uint32_t)*(n>>5));
    }
    ~MyBitMap() {
        delete[] data;
    }
    void setBit(int index, int k);
    int findLeftOne();
	static void initConst();
};