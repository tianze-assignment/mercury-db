#include <iostream>
#include <algorithm>
#include "IndexHandler.h"

using namespace std;

IndexHandler handler;

int key[3], key2[3];
void setkey(int a, int b, int c){
    key[0]=a; key[1]=b; key[2]=c;
}
void setkey2(int a, int b, int c){
    key2[0]=a; key2[1]=b; key2[2]=c;
}

const int N=10000;
const int d=500;
struct S{int a,b,c,id,t;} s[N];
bool cmp(const S&a, const S&b) {
    return a.a!=b.a?a.a<b.a:a.b!=b.b?a.b<b.b:a.c!=b.c?a.c<b.c:a.t<b.t;
}

void output() {
    int c = 0;
    cout << "------------" << endl;
    for(auto it = handler.begin(); !it.isEnd(); ++it)
    {
        //cout << *it << " ";
        if (s[c++].id != *it) cout << "?";
    }
    if (c != N) cout << "bad count" << c << " " << N-d << endl;
    cout << endl;
    cout << "------------" << endl;
}

int main() {
    srand(23333);
    handler.createIndex("1.index", 3);
    
    for (int i=0;i<N;++i) {
        s[i].a=rand() % 10;s[i].b=rand() % 10;s[i].c=rand() % 10;s[i].id=rand()%N;s[i].t=i;
        setkey(s[i].a,s[i].b,s[i].c);
        handler.ins(key, s[i].id);
    }
    random_shuffle(s,s+N);
    for (int i=0; i <d;++i) {
        int x= rand()%N;
        setkey(s[x].a,s[x].b,s[x].c);
        //if (rand()%2) s[i].a=rand()%10, s[i].b=rand()%10, s[i].c=rand()%10;
        setkey2(s[x].a,s[x].b,s[x].c);
        int newid=rand()%N;
        //newid=s[i].id;
        handler.upd(key,s[x].id,key2,newid);
        s[x].id=newid;
    }
    sort(s, s+N, cmp);
    /*
    for (int i=N-d;i<N;++i) {
        setkey(s[i].a,s[i].b,s[i].c);
        handler.del(key,s[i].id);
    }
    sort(s,s+N-d,cmp);
    */
    //for (int i = 0;i<N;++i) printf("%d %d %d %d\n",s[i].a,s[i].b,s[i].c,s[i].id);
/*
    for (int i = 0; i <1000000; ++i) {
        S x;
        x.a=rand()%10;x.b=rand()%10;x.c=rand()%10;x.id=0;
        int k = lower_bound(s,s+N,x,cmp)-s;
        setkey(x.a,x.b,x.c);
        auto it = handler.lowerBound(key);
        if (k==N) {
            if (!it.isEnd())printf("?");
        }
        else if (*it!=s[k].id) printf("?");
    }
*/   
    output();
}