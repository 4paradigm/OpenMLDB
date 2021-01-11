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

static const int kBlockSize = 4 * 1024;

// for compressed snapshot
static const int kCompressBlockSize = 4 * 1024 * 1024;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

// Header is checksum (4 bytes), length (4 bytes), type (1 byte).
static const int kHeaderSizeForCompress = 4 + 4 + 1;

// kHeaderSizeOfCompressBlock should be multiple of 64 bytes
// compress_len(4 bytes), compress_type(1 byte)
static const int kHeaderSizeOfCompressBlock = 64;

}  // namespace log
}  // namespace rtidb

#endif  // SRC_LOG_LOG_FORMAT_H_
