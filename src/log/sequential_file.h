//
// sequential_file.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//


#ifndef RTIDB_LOG_SEQUENTIAL_FILE_H
#define RTIDB_LOG_SEQUENTIAL_FILE_H

#include <stdint.h>
#include <string>

namespace rtidb {

namespace base {
class Status;
class Slice;
}

namespace log {

// A file abstraction for reading sequentially through a file
class SequentialFile {
public:
    SequentialFile() { }
    virtual ~SequentialFile() {}

    // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
    // written by this routine.  Sets "*result" to the data that was
    // read (including if fewer than "n" bytes were successfully read).
    // May set "*result" to point at data in "scratch[0..n-1]", so
    // "scratch[0..n-1]" must be live when "*result" is used.
    // If an error was encountered, returns a non-OK status.
    //
    // REQUIRES: External synchronization
    virtual base::Status Read(size_t n, base::Slice* result, char* scratch) = 0;

    // Skip "n" bytes from the file. This is guaranteed to be no
    // slower that reading the same data, but may be faster.
    //
    // If end of file is reached, skipping will stop at the end of the
    // file, and Skip will return OK.
    //
    // REQUIRES: External synchronization
    virtual base::Status Skip(uint64_t n) = 0;
private:
    // No copying allowed
    SequentialFile(const SequentialFile&);
    void operator=(const SequentialFile&);
};

SequentialFile* NewSeqFile(const std::string& fname, FILE* f);

}
}
#endif /* !RTIDB_LOG_SEQUENTIAL_FILE_H */
