#pragma once

#include "fileio/FileManager.h"
#include "bufmanager/BufPageManager.h"

class FileSystem {
public:
    static FileManager* fm;
    static BufPageManager* bpm;
    static void init();
    static void release();
    static void save();
private:
    static int count;
};