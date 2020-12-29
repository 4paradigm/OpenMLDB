// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "log/log_writer.h"

#include <stdint.h>
#include <snappy.h>
#include "log/coding.h"
#include "log/crc32c.h"
#include "base/glog_wapper.h" // NOLINT
#include "gflags/gflags.h"
#include "base/endianconv.h"
#include "base/compress.h"

DECLARE_string(snapshot_compression);

namespace rtidb {
namespace log {


static void InitTypeCrc(uint32_t* type_crc) {
    for (int i = 0; i <= kMaxRecordType; i++) {
        char t = static_cast<char>(i);
        type_crc[i] = Value(&t, 1);
    }
}

Writer::Writer(WritableFile* dest, bool compressed)
: dest_(dest), block_offset_(0), compressed_(compressed), buffer_(nullptr), compress_buf_(nullptr) {
    InitTypeCrc(type_crc_);
#ifdef PZFPGA_ENABLE
    if (compressed_ &&
            (FLAGS_snapshot_compression == "pz" || FLAGS_snapshot_compression == "snappy")) {
#else
    if (compressed_ &&  FLAGS_snapshot_compression == "snappy") {
#endif
        block_size_ = kCompressBlockSize;
        buffer_ = new char[block_size_];
        compress_buf_ = new char[block_size_];
    } else {
        block_size_ = kBlockSize;
    }
}

Writer::Writer(WritableFile* dest, uint64_t dest_length, bool compressed)
    : dest_(dest), compressed_(compressed), buffer_(nullptr), compress_buf_(nullptr) {
    InitTypeCrc(type_crc_);
#ifdef PZFPGA_ENABLE
    if (compressed_ &&
            (FLAGS_snapshot_compression == "pz" || FLAGS_snapshot_compression == "snappy")) {
#else
    if (compressed_ &&  FLAGS_snapshot_compression == "snappy") {
#endif
        block_size_ = kCompressBlockSize;
        buffer_ = new char[block_size_];
        compress_buf_ = new char[block_size_];
    } else {
        block_size_ = kBlockSize;
    }
    block_offset_ = dest_length % block_size_;
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
        const int leftover = block_size_ - block_offset_;
        assert(leftover >= 0);
        if (leftover < kHeaderSize) {
            // Switch to a new block
            if (leftover > 0) {
                // Fill the trailer (literal below relies on kHeaderSize being
                // 7)
                assert(kHeaderSize == 7);
                Slice fill_slice("\x00\x00\x00\x00\x00\x00", leftover);
#ifdef PZFPGA_ENABLE
                if (!compressed_ ||
                        (FLAGS_snapshot_compression != "pz" &&
                         FLAGS_snapshot_compression != "snappy")) {
#else
                if (!compressed_ || FLAGS_snapshot_compression != "snappy") {
#endif
                    dest_->Append(fill_slice);
                } else {
                    memcpy(buffer_ + block_offset_, fill_slice.data(), leftover);
                    CompressRecord();
                }
            }
            block_offset_ = 0;
        }
        // Invariant: we never leave < kHeaderSize bytes in a block.
        assert(block_size_ - block_offset_ - kHeaderSize >= 0);
        const size_t avail = block_size_ - block_offset_ - kHeaderSize;
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
        const int leftover = block_size_ - block_offset_;
        assert(leftover >= 0);
        if (leftover < kHeaderSize) {
            // Switch to a new block
            if (leftover > 0) {
                // Fill the trailer (literal below relies on kHeaderSize being
                // 7)
                assert(kHeaderSize == 7);
                Slice fill_slice("\x00\x00\x00\x00\x00\x00", leftover);
#ifdef PZFPGA_ENABLE
                if (!compressed_ ||
                        (FLAGS_snapshot_compression != "pz" &&
                         FLAGS_snapshot_compression != "snappy")) {
#else
                if (!compressed_ || FLAGS_snapshot_compression != "snappy") {
#endif
                    dest_->Append(fill_slice);
                } else {
                    memcpy(buffer_ + block_offset_, fill_slice.data(), leftover);
                    CompressRecord();
                }
            }
            block_offset_ = 0;
        }
        // Invariant: we never leave < kHeaderSize bytes in a block.
        assert(block_size_ - block_offset_ - kHeaderSize >= 0);

        const size_t avail = block_size_ - block_offset_ - kHeaderSize;
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
    assert(n <= 0xffff);  // Must fit in two bytes
    assert(static_cast<int>(block_offset_ + kHeaderSize + n) <= block_size_);
    // Format the header
    char buf[kHeaderSize];
    buf[4] = static_cast<char>(n & 0xff);
    buf[5] = static_cast<char>(n >> 8);
    buf[6] = static_cast<char>(t);

    // Compute the crc of the record type and the payload.
    uint32_t crc = Extend(type_crc_[t], ptr, n);
    crc = Mask(crc);  // Adjust for storage
    EncodeFixed32(buf, crc);

#ifdef PZFPGA_ENABLE
    if (!compressed_ ||
            (FLAGS_snapshot_compression != "pz" &&
             FLAGS_snapshot_compression != "snappy")) {
#else
    if (!compressed_ || FLAGS_snapshot_compression != "snappy") {
#endif
        // Write the header and the payload
        Status s = dest_->Append(Slice(buf, kHeaderSize));
        if (s.ok()) {
            s = dest_->Append(Slice(ptr, n));
            if (s.ok()) {
                s = dest_->Flush();
            }
        }
        if (!s.ok()) {
            PDLOG(WARNING, "write error. %s", s.ToString().c_str());
        }

        block_offset_ += kHeaderSize + n;
        return s;
    } else {
        memcpy(buffer_ + block_offset_, &buf, kHeaderSize);
        memcpy(buffer_ + block_offset_ + kHeaderSize, ptr, n);
        block_offset_ += kHeaderSize + n;
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
    // compress
    int compress_len = -1;
#ifdef PZFPGA_ENABLE
    if (FLAGS_snapshot_compression == "pz") {
        FPGA_env* fpga_env = rtidb::base::Compress::GetFpgaEnv();
        compress_len = gzipfpga_compress_nohuff(
                fpga_env, buffer_, compress_buf_, block_size_, block_size_, 0);
    } else {
#endif
        size_t tmp_val = 0;
        snappy::RawCompress(buffer_, block_size_, compress_buf_, &tmp_val);
        compress_len = static_cast<int>(tmp_val);
#ifdef PZFPGA_ENABLE
    }
#endif
    if (compress_len < 0) {
        s = Status::InvalidRecord(Slice("compress failed"));
        PDLOG(WARNING, "write error. %s", s.ToString().c_str());
        return s;
    }
    PDLOG(INFO, "compress_len: %d", compress_len);
    // fill compressed data's header
    char head_of_compress[kHeaderSizeOfCompressData];
    memrev32ifbe(static_cast<void*>(&compress_len));
    memcpy(head_of_compress, static_cast<void*>(&compress_len), sizeof(int));
    memset(head_of_compress + sizeof(int), 0, kHeaderSizeOfCompressData - sizeof(int));
    // write header and compressed data
    s = dest_->Append(Slice(head_of_compress, kHeaderSizeOfCompressData));
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

}  // namespace log
}  // namespace rtidb
