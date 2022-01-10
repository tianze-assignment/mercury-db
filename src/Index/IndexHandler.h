#pragma once

#include <vector>

#include "FileSystem.h"
#include "RecordHandler.h"

using namespace std;

class IndexHandler {
public:
	IndexHandler();
	~IndexHandler();
	int createIndex(const char* fileName, int numKey);
	int openIndex(const char* fileName, int numKey);

	void ins(const int* keys, int val);
	void del(const int* keys, int val);
	void upd(const int* oldKeys, int oldVal, const int* newKeys, int newVal);

	class Iterator {
	public:
		int operator*();
        Iterator& operator++();
        Iterator operator++(int);
        bool isEnd() const;
		bool operator==(const Iterator& it) const;
	private:
		friend class IndexHandler;
		IndexHandler* _handler;
		vector<pair<int,int>> _stack;
		Iterator(IndexHandler* handler):_handler(handler){}
	};
	
	Iterator begin();
	Iterator lowerBound(const int* keys);
	Iterator upperBound(const int* keys);
	Iterator find(const int* keys);

private:
	FileManager* _fm;
	BufPageManager* _bpm;
	int _numKey, _nodeSize, _endPage;
    int _fileID, _pageIndex;
    int* _data;
	void _init(int numKey);
	inline void _moveKeys(int* dest, const int* source);
    void _openPage(int page);
	inline int* _dataKeys(int slot);
	inline int& _dataVal(int slot);
	bool _less(const int* keys1, const int* keys2);
	int _lowerBound(int begin, int end, const int* keys);
	int _upperBound(int begin, int end, const int* keys);
	int _getVal(const Iterator& it);
	void _toLLeaf(Iterator& it);
	void _toNext(Iterator& it);
};