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

#ifndef SRC_LOG_WRITABLE_FILE_H_
#define SRC_LOG_WRITABLE_FILE_H_

#include <string>

namespace openmldb {

namespace base {
class Slice;
}  // namespace base

namespace log {

class Status;
class WritableFile {
 public:
    WritableFile() { wsize_ = 0; }
    virtual ~WritableFile() {}

    virtual Status Append(const base::Slice& data) = 0;
    virtual Status Close() = 0;
    virtual Status Flush() = 0;
    virtual Status Sync() = 0;
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
}  // namespace openmldb

#endif  // SRC_LOG_WRITABLE_FILE_H_
