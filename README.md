# MercuryDB

## Build

### Environment

- Features of C++17 & C++20 are used
- Only tested in Ubuntu 20.04 with GCC 9.3.0 x86_64-linux-gnu

### Requirements

- CMake >= 3.16
- GNU Readline: run `sudo apt install libreadline-dev` for Ubuntu

### Steps

In the root directory of the project, do:
- `mkdir build && cd build`
- `cmake ..`
- `make`

The executable file `DB` can be found at `build/src/`

## Usage

> No arguments are required

### Storage

All database files are stored at directory `databases/` relative to the working directory.

### CLI

- DO NOT press `Ctrl+C` to force quit. Otherwise some data in cache could be lost.
- Press `Ctrl+D` or input `q;` to exit database.
- Multiline input are supported. Input will be regarded as the next line unless ending with `;`.
  - `Ctrl+D` can also be used to abandon a halfway multiline input when there is no other characters on the current line.

## Acknowledgment

- [Hash function from Stack Overflow](https://stackoverflow.com/a/29855973) that use vector of ints as key of unordered set
- [String format from Stack Overflow](https://stackoverflow.com/a/26221725)
- [libfort](https://github.com/seleznevae/libfort) that creates formatted ASCII tables

