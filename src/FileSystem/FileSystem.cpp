#include "FileSystem.h"


FileManager* FileSystem::fm;
BufPageManager* FileSystem::bpm;
int FileSystem::count = 0;

void FileSystem::init() {
    if (!count++) {        
        MyBitMap::initConst();
        fm = new FileManager();
        bpm = new BufPageManager(fm);
    }
}

void FileSystem::release() {
    if (!--count) {    
        bpm->close();
        delete bpm;
        delete fm;
    }
}

void FileSystem::save() {
    bpm->close();
}