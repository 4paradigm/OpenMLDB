//
// log_appender.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//


#ifndef RTIDB_FILE_APPENDER_H
#define RTIDB_FILE_APPENDER_H

#include <string>
#include <stdint.h>
#include <stdio.h>

namespace rtidb {
namespace replica {

class FileAppender {

public:

    FileAppender(const std::string& filename,
                const std::string& folder,
                uint64_t max_size);

    ~FileAppender();

    bool Init();

    uint32_t Append(const char* buf, 
                   uint32_t size);

    bool Flush();

    bool Sync();

    void Close();

    bool IsFull();

private:

    std::string filename_;

    std::string folder_;

    uint64_t current_size_;
    uint64_t max_size_;
    FILE* fd_;
    int fd_no_;

};

}
}
#endif /* !FILE_APPENDER_H */
