//
// sequential_file.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "log/sequential_file.h"

#include <errno.h>
#include <stdio.h>
#include "base/slice.h"
#include "base/status.h"
#include "logging.h"

using ::rtidb::base::Slice;
using ::rtidb::base::Status;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace log {

class PosixSequentialFile: public SequentialFile {
private:
    std::string filename_;
    FILE* file_;

public:
    PosixSequentialFile(const std::string& fname, FILE* f)
        : filename_(fname), file_(f) { }

    virtual ~PosixSequentialFile() { fclose(file_); }

    virtual Status Read(size_t n, Slice* result, char* scratch) {
        Status s;
        size_t r = fread_unlocked(scratch, 1, n, file_);
        *result = Slice(scratch, r);
        if (r < n) {
            if (feof(file_)) {
                // We leave status as ok if we hit the end of the file
            } else {
                // A partial read with an error: return a non-ok status
                s = Status::IOError(filename_, strerror(errno));
            }
        }
        return s;
    }

    virtual Status Skip(uint64_t n) {
        if (fseek(file_, n, SEEK_CUR)) {
            return Status::IOError(filename_, strerror(errno));
        }
        return Status::OK();
    }

    virtual Status Tell(uint64_t* pos) {
        if (pos == NULL) {
            return Status::InvalidArgument("invalid pos arg");
        }
        int64_t ret = ftell(file_);
        if (ret < 0) {
            return Status::IOError("fail to ftell file", strerror(errno));
        }
        *pos = (uint64_t)ret;
        PDLOG(DEBUG, "tell file with pos %lld", ret);
        return Status::OK();
    }

    virtual Status Seek(uint64_t pos) {
        int32_t ret = fseek(file_, pos, SEEK_SET);
        if (ret == 0) {
            return Status::OK();
        }
        return Status::IOError("fail to seek", strerror(errno));
    }

};

SequentialFile* NewSeqFile(const std::string& fname, FILE* f) {
    return new PosixSequentialFile(fname, f);
}

}
}


