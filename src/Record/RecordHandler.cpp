#include <iostream>
#include <cstring>

#include "Record.h"
#include "RecordHandler.h"

const uint16_t FLAG_BITS = (1<<15) | (1<<14);
const uint16_t FILE_END = (1<<15) | (1<<14);
const uint16_t PAGE_END = (1<<15);
const uint16_t EMPTY_SLOT = (1<<14);

RecordHandler::RecordHandler() {
    FileSystem::init();
    _fm = FileSystem::fm;
    _bpm = FileSystem::bpm;
}

RecordHandler::~RecordHandler() {
    FileSystem::release();
}

int RecordHandler::createFile(const char* fileName, const RecordType& type) {
    int flag = 0;
    flag |= !_fm->createFile(fileName);
    flag |= !_fm->openFile(fileName, _fileID);
    _type = type;
    _data = (uint8_t*)_bpm->allocPage(_fileID, 0, _pageIndex, false);
    _bpm->markDirty(_pageIndex);
    _setOffset(0, FILE_END);
    _end = Iterator(this, 0, 0);
    return flag;
}

int RecordHandler::openFile(const char* fileName, const RecordType& type) {
    int flag = 0;
    flag |= !_fm->openFile(fileName, _fileID);
    _type = type;
    for (_end = begin(); !_end.isEnd(); ++_end);
    return flag;
}

RecordHandler::Iterator RecordHandler::begin() {
    int page = 0, slot = 0;
    _nextSlot(page, slot);
    return RecordHandler::Iterator(this, page, slot);
}

void RecordHandler::_openPage(int page) {
    _data = (uint8_t*)_bpm->getPage(_fileID, page, _pageIndex);
}

uint16_t RecordHandler::_getOffset(int slot) {
    return *(uint16_t*)(&_data[PAGE_SIZE-(slot+1<<1)]);
}

void RecordHandler::_setOffset(int slot, uint16_t offset) {
    *(uint16_t*)(&_data[PAGE_SIZE-(slot+1<<1)]) = offset;
}

Record RecordHandler::_getRecord(int page, int slot) {
    _openPage(page);
    int offset = _getOffset(slot);
    if (offset >= PAGE_SIZE) {
        std::cerr << "bad slot";
        exit(-1);
    }

    Record record(_type);
    
    int bitOffset = 0;
    for (int i = 0; i < _type.num_int; ++i) {
        record.int_null[i] = _data[offset] & (1<<bitOffset);
        if (bitOffset < 7) ++bitOffset;
        else ++offset, bitOffset = 0;
    }
    for (int i = 0; i < _type.num_varchar; ++i) {
        record.varchar_null[i] = _data[offset] & (1<<bitOffset);
        if (bitOffset < 7) ++bitOffset;
        else ++offset, bitOffset = 0;
    }
    if (bitOffset) ++offset;

    for (int i = 0; i < _type.num_int; ++i) {
        record.int_data[i] = *(int*)(&_data[offset]);
        offset += sizeof(int);
    }
    for (int i = 0; i < _type.num_varchar; ++i) if (!record.varchar_null[i]) {
        uint16_t len = *(uint16_t*)(&_data[offset]);
        offset += sizeof(uint16_t);
        record.varchar_data[i] = new char[len+1];
        memcpy(record.varchar_data[i], _data+offset, len);
        record.varchar_data[i][len] = 0;
        offset += len;
    }

    return record;
}

void RecordHandler::_nextSlot(int& page, int& slot) {
    _openPage(page);
    while (true) {
        uint16_t offset = _getOffset(slot);
        if ((offset & FLAG_BITS) == PAGE_END) {_openPage(++page); slot = 0;}
        else if ((offset & FLAG_BITS) == EMPTY_SLOT) ++slot;
        else return;
    }
}

int RecordHandler::_getLen(const Record& record) {
    int len = (_type.num_int + _type.num_varchar + 7 >> 3) + sizeof(int) * _type.num_int;
    for (int i = 0; i < _type.num_varchar; ++i) if(!record.varchar_null[i])
        len += sizeof(uint16_t) + strlen(record.varchar_data[i]);
    return len;
}

void RecordHandler::_setRecord(int offset, const Record& record) {
    int bitOffset = 0;
    for (int i = 0; i < _type.num_int; ++i) {
        if (bitOffset == 0) _data[offset] = 0;
        _data[offset] |= record.int_null[i] << bitOffset;
        if (bitOffset < 7) ++bitOffset;
        else ++offset, bitOffset = 0;
    }
    for (int i = 0; i < _type.num_varchar; ++i) {
        if (bitOffset == 0) _data[offset] = 0;
        _data[offset] |= record.varchar_null[i] << bitOffset;
        if (bitOffset < 7) ++bitOffset;
        else ++offset, bitOffset = 0;
    }
    if (bitOffset) ++offset;

    for (int i = 0; i < _type.num_int; ++i) {
        *(int*)(&_data[offset]) = record.int_data[i];
        offset += sizeof(int);
    }
    for (int i = 0; i < _type.num_varchar; ++i) if (!record.varchar_null[i]) {
        uint16_t len = strlen(record.varchar_data[i]);
        *(uint16_t*)(&_data[offset]) = len;
        offset += sizeof(uint16_t);
        memcpy(_data+offset, record.varchar_data[i], len);
        offset += len;
    }
}

RecordHandler::Iterator RecordHandler::ins(const Record& record) {
    _openPage(_end._page);
    int offset = _getOffset(_end._slot) & ~FLAG_BITS;
    int len = _getLen(record);
    if (offset + len > PAGE_SIZE - (_end._slot + 1 << 1)) {
        _bpm->markDirty(_pageIndex);
        _setOffset(_end._slot, PAGE_END | offset);
        _data = (uint8_t*)_bpm->allocPage(_fileID, ++_end._page, _pageIndex, false);
        _end._slot = 0;
        offset = 0;
    }

    _bpm->markDirty(_pageIndex);
    _setOffset(_end._slot, offset);
    _setRecord(offset, record);
    _setOffset(++_end._slot, FILE_END | (offset + len));
    return Iterator(this, _end._page, _end._slot-1);
}

void RecordHandler::del(const Iterator& it) {
    _openPage(it._page);
    int offset = _getOffset(it._slot);
    _bpm->markDirty(_pageIndex);
    _setOffset(it._slot, EMPTY_SLOT | offset);
}

RecordHandler::Iterator RecordHandler::upd(const Iterator& it, const Record& record) {
    _openPage(it._page);
    int offset = _getOffset(it._slot);
    int nextOffset = _getOffset(it._slot + 1) & ~FLAG_BITS;
    _bpm->markDirty(_pageIndex);
    if (offset + _getLen(record) > nextOffset) {
        _setOffset(it._slot, EMPTY_SLOT | offset);
        return ins(record);
    }
    _setRecord(offset, record);
    return it;
}

Record RecordHandler::Iterator::operator*() {
    return _handler->_getRecord(_page, _slot);
}

RecordHandler::Iterator& RecordHandler::Iterator::operator++() {
    ++_slot;
    _handler->_nextSlot(_page, _slot);
    return *this;
}

RecordHandler::Iterator RecordHandler::Iterator::operator++(int) {
    RecordHandler::Iterator it = *this;
    ++_slot;
    _handler->_nextSlot(_page, _slot);
    return it;
}

bool RecordHandler::Iterator::isEnd() {
    _handler->_openPage(_page);
    return (_handler->_getOffset(_slot) & FLAG_BITS) == FILE_END;
}

int RecordHandler::Iterator::toInt() {
    return _page * PAGE_SIZE + _slot;
}

RecordHandler::Iterator::Iterator(RecordHandler* handler, int x):
    RecordHandler::Iterator(handler, x / PAGE_SIZE, x % PAGE_SIZE) {}