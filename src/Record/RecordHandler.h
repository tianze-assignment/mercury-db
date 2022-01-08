#pragma once

#include "FileSystem.h"
#include "Record.h"

class RecordHandler {
public:
    RecordHandler();
    ~RecordHandler();
    int createFile(const char* fileName, const RecordType& type);
    int openFile(const char* fileName, const RecordType& type);

    class Iterator {
    public:
        Record operator*();
        Iterator& operator++();
        Iterator operator++(int);
        bool isEnd();
        int toInt();
        Iterator(RecordHandler* handler, int);
    private:
        friend class RecordHandler;
        RecordHandler* _handler;
        int _page, _slot;
        Iterator(RecordHandler* handler, int page, int slot)
            :_handler(handler), _page(page), _slot(slot){}
        Iterator():Iterator(NULL, 0, 0){}
    };

    Iterator begin();
    Iterator ins(const Record& record);
    void del(const Iterator& it);
    Iterator upd(const Iterator& it, const Record& record);

private:
	FileManager* _fm;
	BufPageManager* _bpm;
    int _fileID, _pageIndex;
    Iterator _end;
    RecordType _type;
    uint8_t* _data;
    void _openPage(int page);
    uint16_t _getOffset(int slot);
    void _setOffset(int slot, uint16_t offset);
    Record _getRecord(int page, int slot);
    void _nextSlot(int& page, int& slot);
};