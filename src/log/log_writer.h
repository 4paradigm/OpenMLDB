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

#ifndef SRC_LOG_LOG_WRITER_H_
#define SRC_LOG_LOG_WRITER_H_

#include <stdint.h>

#include <string>
#include <memory>

#include "base/slice.h"
#include "log/status.h"
#include "log/log_format.h"
#include "log/writable_file.h"

using ::openmldb::base::Slice;

namespace openmldb {
namespace log {

class Writer {
 public:
    // Create a writer that will append data to "*dest".
    // "*dest" must be initially empty.
    // "*dest" must remain live while this Writer is in use.
    Writer(const std::string& compress_type, WritableFile* dest);

    // Create a writer that will append data to "*dest".
    // "*dest" must have initial length "dest_length".
    // "*dest" must remain live while this Writer is in use.
    Writer(const std::string& compress_type, WritableFile* dest, uint64_t dest_length);

    ~Writer();

    Status AddRecord(const Slice& slice);
    Status EndLog();

    inline CompressType GetCompressType() { return compress_type_; }

    inline uint32_t GetBlockSize() { return block_size_; }

    inline uint32_t GetHeaderSize() { return header_size_; }

    CompressType GetCompressType(const std::string& compress_type);

 private:
    WritableFile* dest_;
    uint32_t block_offset_;  // Current offset in block

    // crc32c values for all supported record types.  These are
    // pre-computed to reduce the overhead of computing the crc of the
    // record type stored in the header.
    uint32_t type_crc_[kMaxRecordType + 1];

    CompressType compress_type_;
    uint32_t block_size_;
    const uint32_t header_size_;
    // buffer of kCompressBlockSize
    char* buffer_;
    // buffer for compressed block
    char* compress_buf_;
    Status CompressRecord();
    Status AppendInternal(WritableFile* wf, int leftover);

    Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

    // No copying allowed
    Writer(const Writer&);
    void operator=(const Writer&);
};

struct WriteHandle {
    FILE* fd_;
    WritableFile* wf_;
    Writer* lw_;
    WriteHandle(const std::string& compress_type, const std::string& fname, FILE* fd, uint64_t dest_length = 0)
        : fd_(fd), wf_(NULL), lw_(NULL) {
        wf_ = ::openmldb::log::NewWritableFile(fname, fd);
        lw_ = new Writer(compress_type, wf_, dest_length);
    }

    Status Write(const ::openmldb::base::Slice& slice) { return lw_->AddRecord(slice); }

    Status Sync() { return wf_->Sync(); }

    Status EndLog() { return lw_->EndLog(); }

    uint64_t GetSize() { return wf_->GetSize(); }

    ~WriteHandle() {
        delete lw_;
        delete wf_;
    }
};

std::shared_ptr<WriteHandle> inline CreateWriteHandle(const std::string& compress_type,
        const std::string& fname, const std::string& path) {
    FILE* fd = fopen(path.c_str(), "ab+");
    if (fd == nullptr) {
        return {};
    }
    return std::make_shared<WriteHandle>(compress_type, fname, fd);
}

}  // namespace log
}  // namespace openmldb

#endif  // SRC_LOG_LOG_WRITER_H_
