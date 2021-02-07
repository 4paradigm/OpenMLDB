// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef SRC_LOG_LOG_FORMAT_H_
#define SRC_LOG_LOG_FORMAT_H_

namespace rtidb {
namespace log {

enum RecordType {
    // Zero is reserved for preallocated files
    kZeroType = 0,

    kFullType = 1,

    // For fragments
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4,
    // The end of log file
    kEofType = 5
};

enum CompressType {
    kNoCompress = 0,
    kPz = 1,
    kZlib = 2,
    kSnappy = 3
};

static const int kMaxRecordType = kEofType;

static const uint32_t kBlockSize = 4 * 1024;

// for compressed snapshot
static const uint32_t kCompressBlockSize = 1 * 1024 * 1024;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const uint32_t kHeaderSize = 4 + 2 + 1;

// Header is checksum (4 bytes), length (4 bytes), type (1 byte).
static const uint32_t kHeaderSizeForCompress = 4 + 4 + 1;

// kHeaderSizeOfCompressBlock should be multiple of 64 bytes
// compress_len(4 bytes), compress_type(1 byte)
static const uint32_t kHeaderSizeOfCompressBlock = 64;

static const std::string PZ_COMPRESS_SUFFIX = ".pz";  // NOLINT
static const std::string ZLIB_COMPRESS_SUFFIX = ".zlib";  // NOLINT
static const std::string SNAPPY_COMPRESS_SUFFIX = ".snappy";  // NOLINT

}  // namespace log
}  // namespace rtidb

#endif  // SRC_LOG_LOG_FORMAT_H_
