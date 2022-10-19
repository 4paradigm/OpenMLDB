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

#include "log/log_writer.h"

#include <snappy.h>
#include <stdint.h>
#include <zlib.h>

#include "base/endianconv.h"
#include "base/glog_wrapper.h"
#include "log/coding.h"
#include "log/crc32c.h"

namespace openmldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
    for (int i = 0; i <= kMaxRecordType; i++) {
        char t = static_cast<char>(i);
        type_crc[i] = Value(&t, 1);
    }
}

Writer::Writer(const std::string& compress_type, WritableFile* dest)
    : dest_(dest),
      block_offset_(0),
      compress_type_(GetCompressType(compress_type)),
      header_size_(compress_type_ != kNoCompress ? kHeaderSizeForCompress : kHeaderSize),
      buffer_(nullptr),
      compress_buf_(nullptr) {
    InitTypeCrc(type_crc_);
    if (compress_type_ != kNoCompress) {
        block_size_ = kCompressBlockSize;
        buffer_ = new char[block_size_];
        compress_buf_ = new char[block_size_];
    } else {
        block_size_ = kBlockSize;
    }
    DLOG(INFO) << "block_size_: " << block_size_ << ", "
               << "header_size_: " << header_size_ << ", "
               << "compress_type_: " << compress_type_;
}

Writer::Writer(const std::string& compress_type, WritableFile* dest, uint64_t dest_length)
    : dest_(dest),
      compress_type_(GetCompressType(compress_type)),
      header_size_(compress_type_ != kNoCompress ? kHeaderSizeForCompress : kHeaderSize),
      buffer_(nullptr),
      compress_buf_(nullptr) {
    InitTypeCrc(type_crc_);
    if (compress_type_ != kNoCompress) {
        block_size_ = kCompressBlockSize;
        buffer_ = new char[block_size_];
        compress_buf_ = new char[block_size_];
    } else {
        block_size_ = kBlockSize;
    }
    block_offset_ = dest_length % block_size_;
    DLOG(INFO) << "block_size_: " << block_size_ << ", "
               << "header_size_: " << header_size_ << ", "
               << "compress_type_: " << compress_type_;
}

Writer::~Writer() {
    if (buffer_) {
        delete[] buffer_;
    }
    if (compress_buf_) {
        delete[] compress_buf_;
    }
}

Status Writer::EndLog() {
    Slice slice;
    const char* ptr = slice.data();
    size_t left = 0;
    Status s;
    do {
        const int32_t leftover = block_size_ - block_offset_;
        assert(leftover >= 0);
        if (leftover < static_cast<int32_t>(header_size_)) {
            // Switch to a new block
            if (leftover > 0) {
                // Fill the trailer (literal below relies on header_size_ being
                // 7 or 9)
                if (compress_type_ == kNoCompress) {
                    assert(header_size_ == 7);
                } else {
                    assert(header_size_ == 9);
                }
                s = AppendInternal(dest_, leftover);
                if (!s.ok()) {
                    return s;
                }
            }
            block_offset_ = 0;
        }
        // Invariant: we never leave < header_size_ bytes in a block.
        assert(block_size_ - block_offset_ - header_size_ >= 0);
        const size_t avail = block_size_ - block_offset_ - header_size_;
        const size_t fragment_length = (left < avail) ? left : avail;
        RecordType type = kEofType;
        PDLOG(INFO, "end log");
        s = EmitPhysicalRecord(type, ptr, fragment_length);
        ptr += fragment_length;
        left -= fragment_length;
    } while (s.ok() && left > 0);
    return s;
}

Status Writer::AddRecord(const Slice& slice) {
    const char* ptr = slice.data();
    size_t left = slice.size();

    // Fragment the record if necessary and emit it.  Note that if slice
    // is empty, we still want to iterate once to emit a single
    // zero-length record
    Status s;
    bool begin = true;
    do {
        const int32_t leftover = block_size_ - block_offset_;
        assert(leftover >= 0);
        if (leftover < static_cast<int32_t>(header_size_)) {
            // Switch to a new block
            if (leftover > 0) {
                // Fill the trailer (literal below relies on header_size_ being
                // 7 or 9)
                if (compress_type_ == kNoCompress) {
                    assert(header_size_ == 7);
                } else {
                    assert(header_size_ == 9);
                }
                s = AppendInternal(dest_, leftover);
                if (!s.ok()) {
                    return s;
                }
            }
            block_offset_ = 0;
        }
        // Invariant: we never leave < header_size_ bytes in a block.
        assert(block_size_ - block_offset_ - header_size_ >= 0);

        const size_t avail = block_size_ - block_offset_ - header_size_;
        const size_t fragment_length = (left < avail) ? left : avail;

        RecordType type;
        const bool end = (left == fragment_length);
        if (begin && end) {
            type = kFullType;
        } else if (begin) {
            type = kFirstType;
        } else if (end) {
            type = kLastType;
        } else {
            type = kMiddleType;
        }
        s = EmitPhysicalRecord(type, ptr, fragment_length);
        ptr += fragment_length;
        left -= fragment_length;
        begin = false;
    } while (s.ok() && left > 0);
    return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
    if (compress_type_ == kNoCompress) {
        assert(n <= 0xffff);  // Must fit in two bytes
    } else {
        assert(n <= 0xffffffff);  // Must fit in four bytes
    }
    assert(block_offset_ + header_size_ + n <= static_cast<size_t>(block_size_));
    // Format the header
    char buf[header_size_];  // NOLINT
    memset(buf, 0, header_size_ * sizeof(char));
    if (compress_type_ == kNoCompress) {
        buf[4] = static_cast<char>(n & 0xff);
        buf[5] = static_cast<char>(n >> 8);
        buf[6] = static_cast<char>(t);
    } else {
        buf[4] = static_cast<char>(n & 0xff);
        buf[5] = static_cast<char>((n >> 8) & 0xff);
        buf[6] = static_cast<char>((n >> 16) & 0xff);
        buf[7] = static_cast<char>((n >> 24));
        buf[8] = static_cast<char>(t);
    }
    // Compute the crc of the record type and the payload.
    uint32_t crc = Extend(type_crc_[t], ptr, n);
    crc = Mask(crc);  // Adjust for storage
    EncodeFixed32(buf, crc);

    if (compress_type_ == kNoCompress) {
        // Write the header and the payload
        Status s = dest_->Append(Slice(buf, header_size_));
        if (s.ok()) {
            s = dest_->Append(Slice(ptr, n));
            if (s.ok()) {
                s = dest_->Flush();
            }
        }
        if (!s.ok()) {
            PDLOG(WARNING, "write error. %s", s.ToString().c_str());
        }

        block_offset_ += header_size_ + n;
        return s;
    } else {
        memcpy(buffer_ + block_offset_, &buf, header_size_);
        memcpy(buffer_ + block_offset_ + header_size_, ptr, n);
        block_offset_ += header_size_ + n;
        // fill the trailer if kEofType
        if (t == kEofType) {
            memset(buffer_ + block_offset_, 0, block_size_ - block_offset_);
            block_offset_ = block_size_;
        }
        if (block_offset_ == block_size_) {
            return CompressRecord();
        }
        return Status::OK();
    }
}

Status Writer::CompressRecord() {
    Status s;
    int32_t compress_len = -1;
    switch (compress_type_) {
        case kSnappy: {
            snappy::RawCompress(buffer_, block_size_, compress_buf_, reinterpret_cast<size_t*>(&compress_len));
            break;
        }
        case kZlib: {
            uint32_t dest_len = compressBound(block_size_);

#ifdef __APPLE__
            int res = compress((unsigned char*)compress_buf_, reinterpret_cast<uLongf*>(&dest_len),
                               (const unsigned char*)buffer_, block_size_);
#else

            int res = compress((unsigned char*)compress_buf_, reinterpret_cast<uint64_t*>(&dest_len),
                               (const unsigned char*)buffer_, block_size_);
#endif
            if (res != Z_OK) {
                s = Status::InvalidRecord(Slice("compress failed, error code: " + res));
                PDLOG(WARNING, "write error, compress_type: %d, msg: %s", compress_type_, s.ToString().c_str());
                return s;
            }
            compress_len = static_cast<int32_t>(dest_len);
            break;
        }
        default: {
            s = Status::InvalidRecord(Slice("unsupported compress type: " + compress_type_));
            PDLOG(WARNING, "write error, compress_type: %d, msg: %s", compress_type_, s.ToString().c_str());
            return s;
        }
    }
    if (compress_len < 0) {
        s = Status::InvalidRecord(Slice("compress failed"));
        PDLOG(WARNING, "write error, compress_type: %d, msg: %s", compress_type_, s.ToString().c_str());
        return s;
    }
    DLOG(INFO) << "compress_len: " << compress_len << ", "
               << "compress_type: " << compress_type_;
    // fill compressed data's header
    char head_of_compress[kHeaderSizeOfCompressBlock];
    memrev32ifbe(static_cast<void*>(&compress_len));
    memcpy(head_of_compress, static_cast<void*>(&compress_len), sizeof(int32_t));
    memcpy(head_of_compress + sizeof(int32_t), static_cast<void*>(&compress_type_), 1);
    memset(head_of_compress + sizeof(int32_t) + 1, 0, kHeaderSizeOfCompressBlock - sizeof(int32_t) - 1);
    // write header and compressed data
    s = dest_->Append(Slice(head_of_compress, kHeaderSizeOfCompressBlock));
    if (s.ok()) {
        s = dest_->Append(Slice(compress_buf_, compress_len));
        if (s.ok()) {
            s = dest_->Flush();
        }
    }
    if (!s.ok()) {
        PDLOG(WARNING, "write error. %s", s.ToString().c_str());
    }
    return s;
}

CompressType Writer::GetCompressType(const std::string& compress_type) {
    if (compress_type == "zlib") {
        return kZlib;
    } else if (compress_type == "snappy") {
        return kSnappy;
    } else {
        return kNoCompress;
    }
}

Status Writer::AppendInternal(WritableFile* wf, int32_t leftover) {
    Slice fill_slice("\x00\x00\x00\x00\x00\x00", leftover);
    if (compress_type_ == kNoCompress) {
        wf->Append(fill_slice);
        return Status::OK();
    } else {
        memcpy(buffer_ + block_offset_, fill_slice.data(), leftover);
        return CompressRecord();
    }
}

}  // namespace log
}  // namespace openmldb
