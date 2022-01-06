#include "Record.h"
#include "RecordHandler.h"

#define NO_SLOT_SYMBOL 65535

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
    int pageIndex;
    _type = type;
    flag |= !_fm->createFile(fileName);
    flag |= !_fm->openFile(fileName, _fileID);
    uint16_t* page = (uint16_t*)_bpm->allocPage(_fileID, 0, pageIndex, false);
    _bpm->markDirty(pageIndex);
    page[(PAGE_SIZE >> 1) - 1] = NO_SLOT_SYMBOL;
    return flag;
}

int RecordHandler::openFile(const char* fileName, const RecordType& type) {
    int flag = 0;
    flag |= _fm->openFile(fileName, _fileID);

    return flag;
}