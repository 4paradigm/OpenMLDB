//
// log_appender.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//

#include "replica/file_appender.h"

#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include "logging.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace replica {

FileAppender::FileAppender(const std::string& filename,
                         const std::string& folder,
                         uint64_t max_size):filename_(filename),
    folder_(folder),max_size_(max_size){}


FileAppender::~FileAppender() {

}

bool FileAppender::Init() {
    std::string path = folder_ + "/" + filename_;
    fd_ = fopen(path.c_str(), "ab+");
    if (fd_ == NULL) {
        LOG(WARNING, "fail to create file %s", path.c_str());
        return false;
    }
    fd_no_ = fileno(fd_);
    struct stat sb;
    int ok = fstat(fd_no_, &sb);
    if (ok != 0) {
        LOG(WARNING, "fail to get file %s stat for %s", path.c_str(), strerror(errno));
        return false;
    }
    current_size_ = sb.st_size;
    LOG(INFO, "init file %s in %s with size %lld and fd %d ok", filename_.c_str(),
          folder_.c_str(),
          current_size_, fd_no_);
    return true;
}


uint32_t FileAppender::Append(const char* buf, uint32_t size) {
    if (current_size_ >= max_size_) {
        LOG(WARNING,  "file %s reaches the max size %lld", filename_.c_str(),
            max_size_);
        return -1;
    }

    size_t data_size = fwrite(buf, sizeof(char), size, fd_);

    if (data_size > 0) {
        current_size_ += data_size;
    }

    if (data_size <= 0) {
        LOG(WARNING, "fail to write buf to file %s for %s", filename_.c_str(),
            strerror(errno));
    }

    return data_size;
}


bool FileAppender::Flush() {
    int ok  = fflush(fd_);
    if (ok != 0) {
        LOG(WARNING, "fail to flush buf to file %s for %s",
            filename_.c_str(), strerror(errno));
        return false;
    }
    LOG(DEBUG, "flush write buf to file %s successfully", filename_.c_str());
    return true;
}

bool FileAppender::Sync() {

    if (fd_no_ <= 0) {
        return false;
    }

    int ok = fsync(fd_no_);
    if (ok != 0) {
        LOG(WARNING, "fail to fsync fd %ld with filename %s for %s",
            fd_no_, filename_.c_str(), strerror(errno));
        return false;
    }
    LOG(DEBUG, "sync file %s successfully", filename_.c_str());
    return true;
}

void FileAppender::Close() {
    if (fd_ == NULL) {
        return; 
    }
    int ret = fclose(fd_);
    if (ret != 0) {
        LOG(WARNING, "fail to close filename %s for %s", filename_.c_str(),
            strerror(errno));
        return;
    }
    LOG(DEBUG, "close file %s successfully", filename_.c_str());
    fd_ = NULL;
}

bool FileAppender::IsFull() {
    return current_size_ >= max_size_;
}


}
}



