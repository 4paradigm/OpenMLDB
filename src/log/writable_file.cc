//
// writable_file.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include "log/writable_file.h"

#include "base/slice.h"
#include "base/status.h"

#include <stdio.h>
#include <unistd.h>
#include <errno.h>

using ::rtidb::base::Status;
using ::rtidb::base::Slice;

namespace rtidb {
namespace log {

static Status IOError(const std::string& context, int err_number) {
    return Status::IOError(context, strerror(err_number));
}

class PosixWritableFile : public WritableFile {

public:
    PosixWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

    ~PosixWritableFile() {
        if (file_ != NULL) {
          // Ignoring any potential errors
          fclose(file_);
        }
    }

    virtual Status Append(const Slice& data) {
        size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
        if (r != data.size()) {
            return IOError(filename_, errno);
        }
        wsize_ += r;
        return Status::OK();
    }

    virtual Status Close() {
        Status result;
        if (fclose(file_) != 0) {
          result = IOError(filename_, errno);
        }
        file_ = NULL;
        return result;
    }

    virtual Status Flush() {
        if (fflush_unlocked(file_) != 0) {
            return IOError(filename_, errno);
        }
        return Status::OK();
    }

    virtual Status Sync() {
        // Ensure new files referred to by the manifest are in the filesystem.
        if (fflush_unlocked(file_) != 0 ||
            fdatasync(fileno(file_)) != 0) {
            return IOError(filename_, errno);
        }
        return Status::OK();
    }

private:
    std::string filename_;
    FILE* file_;
};

WritableFile* NewWritableFile(const std::string& fname, FILE* f) {
    return new PosixWritableFile(fname, f);
}

} // end of log
} // end of rtidb


