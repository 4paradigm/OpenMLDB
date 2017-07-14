//
// writable_file.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//


#ifndef RTIDB_LOG_WRITABLE_FILE
#define RTIBD_LOG_WRITABLE_FILE

#include <string>

namespace rtidb {

namespace base {
    class Status;
    class Slice;
}

namespace log {

class WritableFile {
public:
    WritableFile() { }
    virtual ~WritableFile() {}

    virtual base::Status Append(const base::Slice& data) = 0;
    virtual base::Status Close() = 0;
    virtual base::Status Flush() = 0;
    virtual base::Status Sync() = 0;

private:
    // No copying allowed
    WritableFile(const WritableFile&);
    void operator=(const WritableFile&);
};

WritableFile* NewWritableFile(const std::string& fname, FILE* f);


} // end of log
} // end of rtidb

#endif /* !RTIBD_LOG_WRITABLE_FILE_H */
