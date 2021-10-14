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

#ifndef SRC_LOG_SEQUENTIAL_FILE_H_
#define SRC_LOG_SEQUENTIAL_FILE_H_

#include <stdint.h>

#include <string>

namespace openmldb {

namespace base {
class Slice;
}  // namespace base

namespace log {
class Status;

// A file abstraction for reading sequentially through a file
class SequentialFile {
 public:
    SequentialFile() {}
    virtual ~SequentialFile() {}

    // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
    // written by this routine.  Sets "*result" to the data that was
    // read (including if fewer than "n" bytes were successfully read).
    // May set "*result" to point at data in "scratch[0..n-1]", so
    // "scratch[0..n-1]" must be live when "*result" is used.
    // If an error was encountered, returns a non-OK status.
    //
    // REQUIRES: External synchronization
    virtual Status Read(size_t n, base::Slice* result, char* scratch) = 0;

    // Skip "n" bytes from the file. This is guaranteed to be no
    // slower that reading the same data, but may be faster.
    //
    // If end of file is reached, skipping will stop at the end of the
    // file, and Skip will return OK.
    //
    // REQUIRES: External synchronization
    virtual Status Skip(uint64_t n) = 0;

    virtual Status Tell(uint64_t* pos) = 0;
    virtual Status Seek(uint64_t pos) = 0;

 private:
    // No copying allowed
    SequentialFile(const SequentialFile&);
    void operator=(const SequentialFile&);
};

SequentialFile* NewSeqFile(const std::string& fname, FILE* f);

}  // namespace log
}  // namespace openmldb
#endif  // SRC_LOG_SEQUENTIAL_FILE_H_
