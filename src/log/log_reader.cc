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

#include "log/log_reader.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <snappy.h>
#include <stdio.h>
#include <zlib.h>

#include "base/endianconv.h"
#include "base/glog_wrapper.h"
#include "base/strings.h"
#include "log/coding.h"
#include "log/crc32c.h"
#include "log/log_format.h"
#include "log/status.h"

DECLARE_bool(binlog_enable_crc);
DECLARE_int32(binlog_name_length);
DECLARE_string(snapshot_compression);

namespace openmldb {
namespace log {

Reader::Reporter::~Reporter() {}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset, bool compressed)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      buffer_(),
      last_record_offset_(0),
      last_record_end_offset_(0),
      end_of_buffer_offset_(0),
      last_end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0),
      compressed_(compressed),
      uncompress_buf_(nullptr) {
    if (compressed_) {
        block_size_ = kCompressBlockSize;
        uncompress_buf_ = new char[block_size_];
        header_size_ = kHeaderSizeForCompress;
    } else {
        block_size_ = kBlockSize;
        header_size_ = kHeaderSize;
    }
    backing_store_ = new char[block_size_];
    DLOG(INFO) << "block_size_: " << block_size_ << ", "
               << "header_size_: " << header_size_ << ", "
               << "compressed_: " << compressed_;
}

Reader::~Reader() {
    delete[] backing_store_;
    if (uncompress_buf_) {
        delete[] uncompress_buf_;
    }
}

bool Reader::SkipToInitialBlock() {
    size_t offset_in_block = initial_offset_ % block_size_;
    uint64_t block_start_location = initial_offset_ - offset_in_block;

    // Don't search a block if we'd be in the trailer
    if (offset_in_block > static_cast<size_t>(block_size_ - 6)) {
        block_start_location += block_size_;
    }

    end_of_buffer_offset_ = block_start_location;

    // Skip to start of first block that can contain the initial record
    if (block_start_location > 0) {
        Status skip_status = file_->Skip(block_start_location);
        if (!skip_status.ok()) {
            ReportDrop(block_start_location, skip_status);
            return false;
        }
    }
    return true;
}

Status Reader::ReadRecord(Slice* record, std::string* scratch) {
    if (last_record_offset_ < initial_offset_) {
        if (!SkipToInitialBlock()) {
            return Status::IOError("");
        }
    }
    scratch->clear();
    record->clear();
    bool in_fragmented_record = false;
    // Record offset of the logical record that we're reading
    // 0 is a dummy value to make compilers happy
    uint64_t prospective_record_offset = 0;

    Slice fragment;
    while (true) {
        uint64_t offset = 0;
        const unsigned int record_type = ReadPhysicalRecord(&fragment, offset);

        // ReadPhysicalRecord may have only had an empty trailer remaining in
        // its internal buffer. Calculate the offset of the next physical record
        // now that it has returned, properly accounting for its header size.
        uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size() - header_size_ - fragment.size();

        if (resyncing_) {
            if (record_type == kMiddleType) {
                continue;
            } else if (record_type == kLastType) {
                resyncing_ = false;
                continue;
            } else {
                resyncing_ = false;
            }
        }

        switch (record_type) {
            case kFullType:
                if (in_fragmented_record) {
                    // Handle bug in earlier versions of log::Writer where
                    // it could emit an empty kFirstType record at the tail end
                    // of a block followed by a kFullType or kFirstType record
                    // at the beginning of the next block.
                    if (!scratch->empty()) {
                        DEBUGLOG("partial record without end(1)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->clear();
                *record = fragment;
                last_record_offset_ = prospective_record_offset;
                last_record_end_offset_ = end_of_buffer_offset_ - buffer_.size();
                if (offset) {
                    last_end_of_buffer_offset_ = offset;
                }
                return Status::OK();

            case kWaitRecord:
                if (in_fragmented_record) {
                    // This can be caused by the writer dying immediately after
                    // writing a physical record but before completing the next;
                    // don't treat it as a corruption, just ignore the entire
                    // logical record.
                    scratch->clear();
                }
                GoBackToLastBlock();
                return Status::WaitRecord();

            case kFirstType:
                if (in_fragmented_record) {
                    // Handle bug in earlier versions of log::Writer where
                    // it could emit an empty kFirstType record at the tail end
                    // of a block followed by a kFullType or kFirstType record
                    // at the beginning of the next block.
                    if (!scratch->empty()) {
                        DEBUGLOG("partial record without end(2)");
                    }
                }
                prospective_record_offset = physical_record_offset;
                scratch->assign(fragment.data(), fragment.size());
                in_fragmented_record = true;
                break;

            case kMiddleType:
                if (!in_fragmented_record) {
                    DEBUGLOG(
                        "missing start of fragmented record(1). fragment "
                        "size %u",
                        fragment.size());
                } else {
                    scratch->append(fragment.data(), fragment.size());
                }
                break;

            case kLastType:
                if (!in_fragmented_record) {
                    DEBUGLOG(
                        "missing start of fragmented record(2). fragment "
                        "size %u",
                        fragment.size());
                } else {
                    scratch->append(fragment.data(), fragment.size());
                    *record = Slice(*scratch);
                    last_record_offset_ = prospective_record_offset;
                    last_record_end_offset_ = end_of_buffer_offset_ - buffer_.size();
                    if (offset) {
                        last_end_of_buffer_offset_ = offset;
                    }
                    return Status::OK();
                }
                break;

            case kEof:
                if (in_fragmented_record) {
                    // This can be caused by the writer dying immediately after
                    // writing a physical record but before completing the next;
                    // don't treat it as a corruption, just ignore the entire
                    // logical record.
                    scratch->clear();
                }
                return Status::Eof();

            case kBadRecord:
                if (in_fragmented_record) {
                    DEBUGLOG("error in middle of record");
                    in_fragmented_record = false;
                    scratch->clear();
                }
                return Status::InvalidRecord(Slice("kBadRecord"));

            default: {
                char buf[40];
                snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
                DEBUGLOG("%s", buf);
                ReportCorruption((fragment.size() + (in_fragmented_record ? scratch->size() : 0)), buf);
                in_fragmented_record = false;
                scratch->clear();
                return Status::InvalidRecord(Slice(buf, strlen(buf)));
            }
        }
    }
    return Status::IOError("");
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

uint64_t Reader::LastRecordEndOffset() { return last_record_end_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) { ReportDrop(bytes, Status::Corruption(reason)); }

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
    if (reporter_ != NULL && end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
        reporter_->Corruption(static_cast<size_t>(bytes), reason);
    }
}

void Reader::GoBackToLastBlock() {
    size_t offset_in_block = last_end_of_buffer_offset_ % block_size_;
    uint64_t block_start_location = 0;
    if (last_end_of_buffer_offset_ > offset_in_block) {
        block_start_location = last_end_of_buffer_offset_ - offset_in_block;
    }
    DEBUGLOG("go back block from[%lu] to [%lu]", end_of_buffer_offset_, block_start_location);
    end_of_buffer_offset_ = block_start_location;
    buffer_.clear();
    file_->Seek(block_start_location);
}

void Reader::GoBackToStart() {
    uint64_t block_start_location = 0;
    PDLOG(WARNING, "go back block to start");
    end_of_buffer_offset_ = block_start_location;
    buffer_.clear();
    file_->Seek(block_start_location);
}

unsigned int Reader::ReadPhysicalRecord(Slice* result, uint64_t& offset) {
    if (buffer_.size() < static_cast<size_t>(header_size_)) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        Status status;
        if (!compressed_) {
            status = file_->Read(block_size_, &buffer_, backing_store_);
        } else {
            // read header of compressed data
            Slice header_of_compress;
            status = file_->Read(kHeaderSizeOfCompressBlock, &header_of_compress, backing_store_);
            if (!status.ok()) {
                PDLOG(WARNING, "fail to read file %s when reading header", status.ToString().c_str());
                return kWaitRecord;
            }
            const char* data = header_of_compress.data();
            uint32_t compress_len = 0;
            memcpy(static_cast<void*>(&compress_len), data, sizeof(uint32_t));
            memrev32ifbe(static_cast<void*>(&compress_len));
            CompressType compress_type = kNoCompress;
            memcpy(static_cast<void*>(&compress_type), data + sizeof(uint32_t), 1);
            DLOG(INFO) << "compress_len: " << compress_len << ", "
                       << "compress_type: " << compress_type;
            // read compressed data
            Slice block;
            status = file_->Read(compress_len, &block, backing_store_);
            if (!status.ok()) {
                PDLOG(WARNING, "fail to read file %s when reading block", status.ToString().c_str());
                return kWaitRecord;
            }
            const char* block_data = block.data();
            int32_t uncompress_len = 0;
            switch (compress_type) {
                case kSnappy: {
                    snappy::GetUncompressedLength(block_data, static_cast<size_t>(compress_len),
                                                  reinterpret_cast<size_t*>(&uncompress_len));
                    if (!snappy::RawUncompress(block_data, static_cast<size_t>(compress_len), uncompress_buf_)) {
                        PDLOG(WARNING, "bad record when uncompress block, compress type: %d", compress_type);
                        return kBadRecord;
                    }
                    break;
                }
                case kZlib: {
                    uncompress_len = block_size_;
#ifdef __APPLE__
                    int res = uncompress((unsigned char*)uncompress_buf_, reinterpret_cast<uLongf*>(&uncompress_len),
                                         (const unsigned char*)block_data, compress_len);
#else
                    // linux
                    int res = uncompress((unsigned char*)uncompress_buf_, reinterpret_cast<uint64_t*>(&uncompress_len),
                                         (const unsigned char*)block_data, compress_len);
#endif
                    if (res != Z_OK) {
                        PDLOG(WARNING, "bad record when uncompress block, error code: %d, compress type: %d", res,
                              compress_type);
                        return kBadRecord;
                    }
                    break;
                }
                default: {
                    PDLOG(WARNING, "unsupported compress type: %d", compress_type);
                    return kBadRecord;
                }
            }
            if (uncompress_len != static_cast<int32_t>(block_size_)) {
                PDLOG(WARNING, "bad record when uncompress block, uncompress_len: %d, block_size_: %d", uncompress_len,
                      block_size_);
                return kBadRecord;
            }
            DLOG(INFO) << "uncompress_len: " << uncompress_len;
            buffer_ = Slice(uncompress_buf_, block_size_);
        }
        offset = end_of_buffer_offset_;
        end_of_buffer_offset_ += buffer_.size();
        // Read log error
        if (!status.ok()) {
            buffer_.clear();
            ReportDrop(block_size_, status);
            PDLOG(WARNING, "fail to read file %s", status.ToString().c_str());
            return kWaitRecord;
        }
        if (buffer_.size() < static_cast<size_t>(header_size_)) {
            DEBUGLOG("read buffer size[%d] less than header_size_[%d]", buffer_.size(), header_size_);
            return kWaitRecord;
        }
    }

    // Parse the header
    const char* header = buffer_.data();
    unsigned int type = 0;
    uint32_t length = 0;
    if (!compressed_) {
        const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
        const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
        type = header[6];
        length = a | (b << 8);
    } else {
        const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
        const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
        const uint32_t c = static_cast<uint32_t>(header[6]) & 0xff;
        const uint32_t d = static_cast<uint32_t>(header[7]) & 0xff;
        type = header[8];
        length = a | (b << 8) | (c << 16) | (d << 24);
    }
    if (static_cast<size_t>(header_size_ + length) > buffer_.size()) {
        DEBUGLOG("end of file %d, header size %d data length %d", buffer_.size(), header_size_, length);
        return kWaitRecord;
    }
    // Check crc
    if (checksum_) {
        uint32_t expected_crc = Unmask(DecodeFixed32(header));
        uint32_t actual_crc = 0;
        if (!compressed_) {
            actual_crc = Value(header + 6, 1 + length);
        } else {
            actual_crc = Value(header + 8, 1 + length);
        }
        if (actual_crc != expected_crc) {
            // Drop the rest of the buffer since "length" itself may have
            // been corrupted and if we trust it, we could find some
            // fragment of a real log record that just happens to look
            // like a valid log record.
            size_t drop_size = buffer_.size();
            buffer_.clear();
            ReportCorruption(drop_size, "checksum mismatch");
            PDLOG(WARNING, "bad record with crc");
            return kBadRecord;
        }
    }

    // Get Eof flag
    if (type == kEofType && length == 0) {
        buffer_.clear();
        PDLOG(INFO, "end of file");
        return kEof;
    }

    if (type == kZeroType && length == 0) {
        // Skip zero length record without reporting any drops since
        // such records are produced by the mmap based writing code in
        // env_posix.cc that preallocates file regions.
        buffer_.clear();
        PDLOG(WARNING, "bad record with zero type");
        return kBadRecord;
    }

    buffer_.remove_prefix(header_size_ + length);
    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - header_size_ - length < initial_offset_) {
        result->clear();
        PDLOG(WARNING, "bad record with initial_offset");
        return kBadRecord;
    }
    *result = Slice(header + header_size_, length);
    return type;
}

LogReader::LogReader(LogParts* logs, const std::string& log_path, bool compressed) : log_path_(log_path) {
    sf_ = NULL;
    reader_ = NULL;
    logs_ = logs;
    log_part_index_ = -1;
    start_offset_ = 0;
    compressed_ = compressed;

    auto it = logs_->NewIterator();
    it->SeekToLast();
    if (it->Valid()) {
        min_offset_ = it->GetValue();
    } else {
        min_offset_ = UINT64_MAX;
        PDLOG(WARNING, "empty log reader");
    }
}

LogReader::~LogReader() {
    delete sf_;
    delete reader_;
}

bool LogReader::SetOffset(uint64_t start_offset) {
    start_offset_ = start_offset;
    if (start_offset < min_offset_) {
        PDLOG(WARNING, "SetOffset %lu is smaller than the minimum offset %lu in the logs", start_offset, min_offset_);
        return false;
    }
    return true;
}

void LogReader::GoBackToLastBlock() {
    if (sf_ == NULL || reader_ == NULL) {
        return;
    }
    reader_->GoBackToLastBlock();
}

void LogReader::GoBackToStart() {
    if (sf_ == NULL || reader_ == NULL) {
        return;
    }
    reader_->GoBackToStart();
}

int LogReader::GetLogIndex() { return log_part_index_; }

uint64_t LogReader::GetLastRecordEndOffset() {
    if (reader_ == NULL) {
        PDLOG(WARNING, "reader is NULL");
        return 0;
    }
    return reader_->LastRecordEndOffset();
}

::openmldb::log::Status LogReader::ReadNextRecord(::openmldb::base::Slice* record, std::string* buffer) {
    // first read record
    if (sf_ == NULL) {
        if (RollRLogFile() < 0) {
            PDLOG(WARNING, "fail to roll read log");
            return ::openmldb::log::Status::WaitRecord();
        }
    }
    ::openmldb::log::Status status = reader_->ReadRecord(record, buffer);
    if (status.IsEof()) {
        PDLOG(INFO, "reach the end of file. index %d", log_part_index_);
        if (RollRLogFile() < 0) {
            // reache the latest log part
            return status;
        }
        return ::openmldb::log::Status::Eof();
    }
    return status;
}

int LogReader::GetEndLogIndex() {
    int log_index = -1;
    LogParts::Iterator* it = logs_->NewIterator();
    if (logs_->GetSize() <= 0) {
        delete it;
        return log_index;
    }
    it->SeekToFirst();
    if (it->Valid()) {
        log_index = it->GetKey();
    }
    delete it;
    return log_index;
}

int LogReader::RollRLogFile() {
    LogParts::Iterator* it = logs_->NewIterator();
    if (logs_->GetSize() <= 0) {
        delete it;
        PDLOG(WARNING, "no log avaliable");
        return -1;
    }
    it->SeekToFirst();
    // use log entry offset to find the log part file
    int index = -1;
    if (log_part_index_ < 0) {
        while (it->Valid()) {
            DEBUGLOG("log index[%u] and start offset %lld", it->GetKey(), it->GetValue());
            if (it->GetValue() <= start_offset_) {
                break;
            }
            it->Next();
        }
        if (it->Valid()) {
            index = (int)it->GetKey();  // NOLINT
        } else {
            PDLOG(WARNING, "no log part matched! start_offset[%lu]", start_offset_);
        }
    } else {
        uint32_t log_part_index = (uint32_t)log_part_index_;
        while (it->Valid()) {
            DEBUGLOG("log index[%u] and start offset %lu", it->GetKey(), it->GetValue());
            // find the next of current index log file part
            if (it->GetKey() == log_part_index + 1) {
                index = (int)it->GetKey();  // NOLINT
                break;
            } else if (it->GetKey() == log_part_index) {
                DEBUGLOG("no new file. log_part_index %d", log_part_index);
                break;
            }
            uint32_t cur_index = it->GetKey();
            it->Next();
            if (it->Valid() && it->GetKey() <= log_part_index && cur_index > log_part_index) {
                index = (int)cur_index;  // NOLINT
                break;
            }
        }
    }
    delete it;
    if (index >= 0) {
        // open a new log part file
        std::string full_path =
            log_path_ + "/" + ::openmldb::base::FormatToString(index, FLAGS_binlog_name_length) + ".log";
        if (OpenSeqFile(full_path) != 0) {
            return -1;
        }
        delete reader_;
        // roll a new log part file, reset status
        reader_ = new Reader(sf_, NULL, FLAGS_binlog_enable_crc, 0, compressed_);
        PDLOG(INFO, "roll log file from index[%d] to index[%d]", log_part_index_, index);
        log_part_index_ = index;
        return 0;
    }
    return -1;
}

int LogReader::OpenSeqFile(const std::string& path) {
    FILE* fd = fopen(path.c_str(), "rb");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to open file %s", path.c_str());
        return -1;
    }
    // close old Sequentialfile
    if (sf_ != NULL) {
        delete sf_;
        sf_ = NULL;
    }
    PDLOG(INFO, "open log file %s", path.c_str());
    sf_ = ::openmldb::log::NewSeqFile(path, fd);
    return 0;
}

}  // namespace log
}  // namespace openmldb
