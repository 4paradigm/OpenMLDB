/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include "log/writable_file.h"
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include "base/slice.h"
#include "base/status.h"

using ::fedb::base::Slice;
using ::fedb::base::Status;

namespace fedb {
namespace log {

static Status IOError(const std::string& context, int err_number) {
    return Status::IOError(context, strerror(err_number));
}

class PosixWritableFile : public WritableFile {
 public:
    PosixWritableFile(const std::string& fname, FILE* f)
        : filename_(fname), file_(f) {}

    ~PosixWritableFile() {
        if (file_ != NULL) {
            // Ignoring any potential errors
            fclose(file_);
        }
    }

    virtual Status Append(const Slice& data) {
#if __linux__
        size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
#else
        size_t r = fwrite(data.data(), 1, data.size(), file_);
#endif
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
#if __linux__
        if (fflush_unlocked(file_) != 0) {
            return IOError(filename_, errno);
        }
#else
        if (fflush(file_) != 0) {
            return IOError(filename_, errno);
        }
#endif
        return Status::OK();
    }

    virtual Status Sync() {
#if __linux__
        // Ensure new files referred to by the manifest are in the filesystem.
        if (fflush_unlocked(file_) != 0 || fdatasync(fileno(file_)) != 0) {
            return IOError(filename_, errno);
        }
#else
        // Ensure new files referred to by the manifest are in the filesystem.
        if (fflush(file_) != 0 || fsync(fileno(file_)) != 0) {
            return IOError(filename_, errno);
        }
#endif
        return Status::OK();
    }

 private:
    std::string filename_;
    FILE* file_;
};

WritableFile* NewWritableFile(const std::string& fname, FILE* f) {
    return new PosixWritableFile(fname, f);
}

}  // namespace log
}  // namespace fedb
