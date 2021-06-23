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
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef SRC_LOG_LOG_FORMAT_H_
#define SRC_LOG_LOG_FORMAT_H_

namespace openmldb {
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
    kZlib = 1,
    kSnappy = 2
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

static const std::string ZLIB_COMPRESS_SUFFIX = ".zlib";  // NOLINT
static const std::string SNAPPY_COMPRESS_SUFFIX = ".snappy";  // NOLINT

}  // namespace log
}  // namespace openmldb

#endif  // SRC_LOG_LOG_FORMAT_H_
