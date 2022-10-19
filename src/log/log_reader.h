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

#ifndef SRC_LOG_LOG_READER_H_
#define SRC_LOG_LOG_READER_H_

#include <stdint.h>

#include <string>

#include "base/skiplist.h"
#include "base/slice.h"
#include "log/log_format.h"
#include "log/sequential_file.h"

using ::openmldb::base::Slice;

namespace openmldb {
namespace log {

class Status;

class Reader {
 public:
    // Interface for reporting errors.
    class Reporter {
     public:
        virtual ~Reporter();

        // Some corruption was detected.  "size" is the approximate number
        // of bytes dropped due to the corruption.
        virtual void Corruption(size_t bytes, const Status& status) = 0;
    };

    // Create a reader that will return log records from "*file".
    // "*file" must remain live while this Reader is in use.
    //
    // If "reporter" is non-NULL, it is notified whenever some data is
    // dropped due to a detected corruption.  "*reporter" must remain
    // live while this Reader is in use.
    //
    // If "checksum" is true, verify checksums if available.
    //
    // The Reader will start reading at the first record located at physical
    // position >= initial_offset within the file.
    Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset, bool compressed);

    ~Reader();

    // Read the next record into *record.  Returns true if read
    // successfully, false if we hit end of the input.  May use
    // "*scratch" as temporary storage.  The contents filled in *record
    // will only be valid until the next mutating operation on this
    // reader or the next mutation to *scratch.
    Status ReadRecord(Slice* record, std::string* scratch);

    // Returns the physical offset of the last record returned by ReadRecord.
    //
    // Undefined before the first call to ReadRecord.
    uint64_t LastRecordOffset();

    uint64_t LastRecordEndOffset();

    void GoBackToLastBlock();
    void GoBackToStart();

    inline bool GetCompressed() { return compressed_; }

    inline uint32_t GetBlockSize() { return block_size_; }

    inline uint32_t GetHeaderSize() { return header_size_; }

 private:
    SequentialFile* const file_;
    Reporter* const reporter_;
    bool const checksum_;
    char* backing_store_;
    Slice buffer_;

    // Offset of the last record returned by ReadRecord.
    uint64_t last_record_offset_;
    // End offset of the last record returned by ReadRecord.
    uint64_t last_record_end_offset_;
    // Offset of the first location past the end of buffer_.
    uint64_t end_of_buffer_offset_;
    uint64_t last_end_of_buffer_offset_;

    // Offset at which to start looking for the first record to return
    uint64_t initial_offset_;

    // True if we are resynchronizing after a seek (initial_offset_ > 0). In
    // particular, a run of kMiddleType and kLastType records can be silently
    // skipped in this mode
    bool resyncing_;

    bool compressed_;
    uint32_t block_size_;
    uint32_t header_size_;
    // buffer for uncompressed block
    char* uncompress_buf_;

    // Extend record types with the following special values
    enum {
        kEof = kMaxRecordType + 1,
        // Returned whenever we find an invalid physical record.
        // Currently there are three situations in which this happens:
        // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
        // * The record is a 0-length record (No drop is reported)
        // * The record is below constructor's initial_offset (No drop is
        // reported)
        kBadRecord = kMaxRecordType + 2,
        kWaitRecord = kMaxRecordType + 3
    };

    // Skips all blocks that are completely before "initial_offset_".
    //
    // Returns true on success. Handles reporting.
    bool SkipToInitialBlock();

    // Return type, or one of the preceding special values
    unsigned int ReadPhysicalRecord(Slice* result, uint64_t& offset);  // NOLINT

    // Reports dropped bytes to the reporter.
    // buffer_ must be updated to remove the dropped bytes prior to invocation.
    void ReportCorruption(uint64_t bytes, const char* reason);
    void ReportDrop(uint64_t bytes, const Status& reason);

    // No copying allowed
    Reader(const Reader&);
    void operator=(const Reader&);
};

typedef ::openmldb::base::Skiplist<uint32_t, uint64_t, ::openmldb::base::DefaultComparator> LogParts;

class LogReader {
 public:
    LogReader(LogParts* logs, const std::string& log_path, bool compressed);
    virtual ~LogReader();
    Status ReadNextRecord(::openmldb::base::Slice* record, std::string* buffer);
    int RollRLogFile();
    int OpenSeqFile(const std::string& path);
    void GoBackToLastBlock();
    void GoBackToStart();
    int GetLogIndex();
    int GetEndLogIndex();
    uint64_t GetLastRecordEndOffset();
    bool SetOffset(uint64_t start_offset);
    uint64_t GetMinOffset() const {
        return min_offset_;
    }
    LogReader(const LogReader&) = delete;
    LogReader& operator=(const LogReader&) = delete;

 protected:
    std::string log_path_;
    int log_part_index_;
    uint64_t start_offset_;
    uint64_t min_offset_;
    SequentialFile* sf_;
    Reader* reader_;
    LogParts* logs_;
    bool compressed_;
};

}  // namespace log
}  // namespace openmldb

#endif  // SRC_LOG_LOG_READER_H_
