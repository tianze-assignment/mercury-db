#pragma once

#include "Record.h"

#include "fileio/FileManager.h"
#include "bufmanager/BufPageManager.h"

class RecordHandler {
public:
    RecordHandler();
    ~RecordHandler();
    int createFile(const char* fileName, const RecordType& type);
    int openFile(const char* fileName, const RecordType& type);
private:
	FileManager* _fm;
	BufPageManager* _bpm;
    int _fileID;
    RecordType _type;
};