#include "MyBitMap.h"

#include <cstdio>

char MyBitMap::h[61];

void MyBitMap::setBit(int index, int k) {
    printf ("set %d %d\n", index, k);
    if (k) data[index>>5] |= 1U<<(index&31);
    else data[index>>5] &= ~(1U<<(index&31));
}

int MyBitMap::findLeftOne() {
    int res=n;
    for (int i = 0; i < (n>>5); ++i) if (data[i]) {
        res = (i<<5) + h[_hash(_lowbit(data[i]))];
        break;
    }
    printf("res %d %u %d %d %d\n",res, data[0], h[1], h[2], h[3]);
    return res < n ? res : n;
}

void MyBitMap::initConst() {
    printf("init!!\n");
    for (int i = 0; i < 32; ++ i) {
        unsigned int k = (1 << i);
        h[_hash(k)] = i;
    }
}