//
// writable_file.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//

#ifndef SRC_LOG_WRITABLE_FILE_H_
#define SRC_LOG_WRITABLE_FILE_H_

#include <string>

namespace rtidb {

namespace base {
class Status;
class Slice;
}  // namespace base

namespace log {

class WritableFile {
 public:
    WritableFile() { wsize_ = 0; }
    virtual ~WritableFile() {}

    virtual base::Status Append(const base::Slice& data) = 0;
    virtual base::Status Close() = 0;
    virtual base::Status Flush() = 0;
    virtual base::Status Sync() = 0;
    uint64_t GetSize() { return wsize_; }

 protected:
    uint64_t wsize_;

 private:
    // No copying allowed
    WritableFile(const WritableFile&);
    void operator=(const WritableFile&);
};

WritableFile* NewWritableFile(const std::string& fname, FILE* f);

}  // namespace log
}  // namespace rtidb

#endif  // SRC_LOG_WRITABLE_FILE_H_
