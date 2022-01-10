#include <vector>
#include <cstring>

#include "IndexHandler.h"

using namespace std;

const int C_DATA = 0;
const int F_DATA = 1;
const int EXLEN = 2;

const int INDEX_LEAF_BIT = 1<<15;

IndexHandler::IndexHandler() {
    FileSystem::init();
    _fm = FileSystem::fm;
    _bpm = FileSystem::bpm;
}

IndexHandler::~IndexHandler() {
    FileSystem::release();
}

int IndexHandler::createIndex(const char* fileName, int numKey) {
    int flag = 0;
    flag |= !_fm->createFile(fileName);
    flag |= !_fm->openFile(fileName, _fileID);
    _init(numKey);
    _data = (int*)_bpm->allocPage(_fileID, 0, _pageIndex, false);
    _bpm->markDirty(_pageIndex);
    _data[C_DATA] = INDEX_LEAF_BIT;
    _data[F_DATA] = _endPage = 0;
    return flag;
}

int IndexHandler::openIndex(const char* fileName, int numKey) {
    int flag = 0;
    flag |= !_fm->openFile(fileName, _fileID);
    _init(numKey);
    _openPage(0);
    _endPage = _data[F_DATA];
    return flag;
}

void IndexHandler::ins(const int* keys, int val) {
    vector<pair<int,int>> nodes;
    nodes.push_back(make_pair(0,0));
    while (true) {
        _openPage(nodes.back().first);
        if (_data[C_DATA] & INDEX_LEAF_BIT) break;
        int pos = _upperBound(1, _data[C_DATA], keys) - 1;
        if (_less(keys, _dataKeys(pos))) {
            _bpm->markDirty(_pageIndex);
            _moveKeys(_dataKeys(pos), keys);
        }
        nodes.push_back(make_pair(_dataVal(pos), pos));
    }
    int size = _data[C_DATA] & ~INDEX_LEAF_BIT;
    int pos = _upperBound(0, size, keys);
    int* keysBuf = new int[_numKey];
    _moveKeys(keysBuf, keys);
    bool newPage = false;

    while (true) {
        // insert
        _bpm->markDirty(_pageIndex);
        for (int i = size; i > pos; --i) {
            _moveKeys(_dataKeys(i), _dataKeys(i-1));
            _dataVal(i) = _dataVal(i-1);
        }
        _moveKeys(_dataKeys(pos), keysBuf);
        _dataVal(pos) = val;
        ++_data[C_DATA];

        if (size < _nodeSize) break;
        // split
        newPage = true;
        int _page2Index;
        int* _data2 = (int*)_bpm->allocPage(_fileID, ++_endPage, _page2Index, false);
        _bpm->markDirty(_page2Index);
        int size1 = (size>>1) + 1, size2 = size+1 >> 1;
        _data2[C_DATA] = (_data[C_DATA] & INDEX_LEAF_BIT) | size2;
        memcpy(_data2+EXLEN, _dataKeys(size1), (_numKey+1)*size2*sizeof(int));
        _data[C_DATA] -= size2;

        // pushup
        _moveKeys(keysBuf, _dataKeys(size1));
        val = _endPage;
        pos = nodes.back().second + 1;
        nodes.pop_back();
        // is root
        if (nodes.empty()) {
            int _page1Index;
            int* _data1 = (int*)_bpm->allocPage(_fileID, ++_endPage, _page1Index, false);
            _bpm->markDirty(_page1Index);
            memcpy(_data1, _data, (EXLEN + (_numKey+1)*size1) * sizeof(int));

            _data[C_DATA] = 2;
            _dataVal(0) = _endPage;
            _moveKeys(_dataKeys(1), keysBuf);
            _dataVal(1) = val;
            break;
        }
        
        _openPage(nodes.back().first);
        size = _data[C_DATA];
        // continue
    }

    if (newPage) {
        _openPage(0);
        _bpm->markDirty(_pageIndex);
        _data[F_DATA] = _endPage;
    }
    delete[] keysBuf;
}

void IndexHandler::del(const int* keys, int val) {
    Iterator it = lowerBound(keys);
    for(; !it.isEnd(); ++it) if (*it == val) break;
    if (it.isEnd()) {
        cerr << "del index failed" << endl;
        return;
    }
    while (true) {
        int page = it._stack.back().first, slot = it._stack.back().second;
        _openPage(page);
        _bpm->markDirty(_pageIndex);
        int size = _data[C_DATA] & ~INDEX_LEAF_BIT;
        for (int i = slot; i < size - 1; ++i) {
            _moveKeys(_dataKeys(i), _dataKeys(i+1));
            _dataVal(i) = _dataVal(i+1);
        }
        --_data[C_DATA];
        // to be easier, delete node only when empty
        if (page == 0 || size > 1) break;
        it._stack.pop_back();
    }
}

void IndexHandler::upd(const int* oldKeys, int oldVal, const int* newKeys, int newVal) {
    bool updKey = false;
    for (int i = 0; i < _numKey; ++i) if (newKeys[i] != oldKeys[i]) updKey = true;
    if (updKey) {
        del(oldKeys, oldVal);
        ins(newKeys, newVal);
        return;
    }
    Iterator it = lowerBound(oldKeys);
    for(; !it.isEnd(); ++it) if (*it == oldVal) break;
    if (it.isEnd()) {
        cerr << "upd index failed" << endl;
        return;
    }
    int page = it._stack.back().first, slot = it._stack.back().second;
    _openPage(page);
    _bpm->markDirty(_pageIndex);
    _dataVal(slot) = newVal;
}

IndexHandler::Iterator IndexHandler::begin() {
    Iterator it(this);
    _openPage(0);
    if ((_data[C_DATA] & ~INDEX_LEAF_BIT) == 0) return it;
    it._stack.push_back(make_pair(0, 0));
    _toLLeaf(it);
    return it;
}

IndexHandler::Iterator IndexHandler::lowerBound(const int* keys) {
    Iterator it(this);
    int page = 0;
    while (true) {
        _openPage(page);
        if (_data[C_DATA] & INDEX_LEAF_BIT) break;
        int slot = _lowerBound(1, _data[C_DATA], keys) - 1;
        it._stack.push_back(make_pair(page, slot));
        page = _dataVal(slot);
    }
    int size = _data[C_DATA] & ~INDEX_LEAF_BIT;
    int slot = _lowerBound(0, size, keys);
    it._stack.push_back(make_pair(page, slot));
    if (slot == size) _toNext(it);
    return it;
}

IndexHandler::Iterator IndexHandler::upperBound(const int* keys) {
    Iterator it(this);
    int page = 0;
    while (true) {
        _openPage(page);
        if (_data[C_DATA] & INDEX_LEAF_BIT) break;
        int slot = _upperBound(1, _data[C_DATA], keys) - 1;
        it._stack.push_back(make_pair(page, slot));
        page = _dataVal(slot);
    }
    int size = _data[C_DATA] & ~INDEX_LEAF_BIT;
    int slot = _upperBound(0, size, keys);
    it._stack.push_back(make_pair(page, slot));
    if (slot == size) _toNext(it);
    return it;
}

IndexHandler::Iterator IndexHandler::find(const int* keys) {
    Iterator it = lowerBound(keys);
    if (it.isEnd()) return it;
    _openPage(it._stack.back().first);
    auto dataKeys = _dataKeys(it._stack.back().second);
    for (int i = 0; i < _numKey; ++i)
        if (dataKeys[i] != keys[i]) return Iterator(this);
    return it;
}

void IndexHandler::_init(int numKey) {
    _numKey = numKey;
    _nodeSize = (PAGE_INT_NUM - EXLEN)/ (numKey + 1) - 1;
}

void IndexHandler::_moveKeys(int* dest, const int* source) {
    memcpy(dest, source, _numKey*sizeof(int));
}

void IndexHandler::_openPage(int page) {
    _data = (int*)_bpm->getPage(_fileID, page, _pageIndex);
}

int* IndexHandler::_dataKeys(int slot) {
    return _data + EXLEN + (_numKey+1)*slot;
}

int& IndexHandler::_dataVal(int slot) {
    return _data[EXLEN + (_numKey+1)*slot + _numKey];
}

bool IndexHandler::_less(const int* keys1, const int* keys2) {
    for (int i = 0; i < _numKey - 1; ++i)
        if (keys1[i] != keys2[i]) return keys1[i] < keys2[i];
    return keys1[_numKey-1] < keys2[_numKey-1];
}

int IndexHandler::_lowerBound(int begin, int end, const int* keys) {
    int l = begin, r = end-1, res = end, mid;
    while (l <= r) {
        mid = l+r >> 1;
        if (_less(_dataKeys(mid), keys)) l = mid+1;
        else res = mid, r = mid-1;
    }
    return res;
}

int IndexHandler::_upperBound(int begin, int end, const int* keys) {
    int l = begin, r = end-1, res = end, mid;
    while (l <= r) {
        mid = l+r >> 1;
        if (_less(keys, _dataKeys(mid))) res = mid, r = mid-1;
        else l = mid+1;
    }
    return res;
}

int IndexHandler::_getVal(const Iterator& it) {
    _openPage(it._stack.back().first);
    return _dataVal(it._stack.back().second);
}

void IndexHandler::_toLLeaf(Iterator& it) {
    int page = it._stack.back().first, slot = it._stack.back().second;
    _openPage(page);
    while (!(_data[C_DATA] & INDEX_LEAF_BIT)) {
        page = _dataVal(slot); slot = 0;
        it._stack.push_back(make_pair(page, slot));
        _openPage(page);
    }
}

void IndexHandler::_toNext(Iterator& it) {
    while(!it._stack.empty()) {
        int page = it._stack.back().first, slot = it._stack.back().second;
        _openPage(page);
        if (slot + 1 < (_data[C_DATA] & ~INDEX_LEAF_BIT)) {
            it._stack.back() = make_pair(page, slot+1);
            _toLLeaf(it);
            return;
        }
        it._stack.pop_back();
    }
}

int IndexHandler::Iterator::operator*() {
    return _handler->_getVal(*this);
}

IndexHandler::Iterator& IndexHandler::Iterator::operator++() {
    _handler->_toNext(*this);
    return *this;
}

IndexHandler::Iterator IndexHandler::Iterator::operator++(int) {
    Iterator it = *this;
    _handler->_toNext(*this);
    return it;
}

bool IndexHandler::Iterator::isEnd() const{
    return _stack.empty();
}

bool IndexHandler::Iterator::operator==(const Iterator& it) const{
    if (isEnd()) return it.isEnd();
    return _stack.back() == it._stack.back();
}