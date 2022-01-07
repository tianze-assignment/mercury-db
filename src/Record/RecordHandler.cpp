#include <iostream>
#include <cstring>

#include "Record.h"
#include "RecordHandler.h"

const uint16_t FILE_END = 65535;
const uint16_t PAGE_END = 65534;
const uint16_t EMPTY_SLOT = 65533;

RecordHandler::RecordHandler() {
    MyBitMap::initConst();
    _fm = new FileManager();
    _bpm = new BufPageManager(_fm);
}

RecordHandler::~RecordHandler() {
    _bpm->close();
    delete _bpm;
    delete _fm;
}

int RecordHandler::createFile(const char* fileName, const RecordType& type) {
    int flag = 0;
    flag |= !_fm->createFile(fileName);
    flag |= !_fm->openFile(fileName, _fileID);
    _type = type;
    _data = (uint8_t*)_bpm->allocPage(_fileID, 0, _pageIndex, false);
    _bpm->markDirty(_pageIndex);
    _setOffset(0, FILE_END);
    return flag;
}

int RecordHandler::openFile(const char* fileName, const RecordType& type) {
    int flag = 0;
    flag |= !_fm->openFile(fileName, _fileID);
    _type = type;
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
    ++offset;

    for (int i = 0; i < _type.num_int; ++i) {
        record.int_data[i] = *(int*)(&_data[offset]);
        offset += sizeof(int);
    }
    for (int i = 0; i < _type.num_varchar; ++i) {
        uint16_t len = *(uint16_t*)(&_data[offset]);
        offset += sizeof(uint16_t);
        record.varchar_data[i] = new char[len+1];
        memcpy(record.varchar_data[i], _data+offset, len);
        record.varchar_data[i][len] = 0;
    }

    return record;
}

void RecordHandler::_nextSlot(int& page, int& slot) {
    int pageIndex;
    uint16_t* data = NULL;
    while (true) {
        if (data == NULL) data = (uint16_t*)_bpm->getPage(_fileID, page, pageIndex);
        uint16_t offset = data[PAGE_SIZE-slot-1];

        if (offset == FILE_END) return;
        if (offset == PAGE_END) {++page; slot = 0;}
        if (offset == EMPTY_SLOT) ++
    }
}

Record RecordHandler::Iterator::operator*() {
    return _handler->_getRecord(_page, _slot);
}