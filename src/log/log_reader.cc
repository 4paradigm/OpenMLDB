//
// log_reader.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "log/log_reader.h"

#include <stdio.h>
#include "log/coding.h"
#include "log/crc32c.h"
#include "log/log_format.h"
#include "logging.h"
#include "base/status.h"
#include <gflags/gflags.h>
#include "base/strings.h"

using ::baidu::common::INFO;
using ::baidu::common::DEBUG;
using ::baidu::common::WARNING;
using ::rtidb::base::Status;

DECLARE_bool(binlog_enable_crc);
DECLARE_int32(binlog_name_length);

namespace rtidb {
namespace log {

Reader::Reporter::~Reporter() {

}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      last_record_offset_(0),
      last_record_end_offset_(0),
      end_of_buffer_offset_(0),
      last_end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

bool Reader::SkipToInitialBlock() {
  size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    offset_in_block = 0;
    block_start_location += kBlockSize;
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

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

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
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(1)");
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
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
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
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
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
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return Status::Eof();

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return Status::IOError("");
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

uint64_t Reader::LastRecordEndOffset() {
  return last_record_end_offset_;
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != NULL &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

void Reader::GoBackToLastBlock() {
    size_t offset_in_block = last_end_of_buffer_offset_ % kBlockSize;
    uint64_t block_start_location = 0;
    if (last_end_of_buffer_offset_ >  offset_in_block) {
        block_start_location = last_end_of_buffer_offset_ - offset_in_block;
    }
    PDLOG(DEBUG, "go back block from[%lu] to [%lu]", end_of_buffer_offset_, block_start_location);
    end_of_buffer_offset_ = block_start_location;
    buffer_.clear();
    file_->Seek(block_start_location);
}

void Reader::GoBackToStart() {
    uint64_t block_start_location = 0;
    PDLOG(DEBUG, "go back block to start");
    end_of_buffer_offset_ = block_start_location;
    buffer_.clear();
    file_->Seek(block_start_location);
}

unsigned int Reader::ReadPhysicalRecord(Slice* result, uint64_t& offset) {
    if (buffer_.size() < kHeaderSize) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        offset = end_of_buffer_offset_;
        end_of_buffer_offset_ += buffer_.size();
        // Read log error
        if (!status.ok()) {
            buffer_.clear();
            ReportDrop(kBlockSize, status);
            PDLOG(WARNING, "fail to read file %s", status.ToString().c_str());
            return kWaitRecord;
        }
        if (buffer_.size() < kHeaderSize) { 
            PDLOG(DEBUG, "read buffer size[%d] less than kHeaderSize[%d]", 
                            buffer_.size(), kHeaderSize);
            return kWaitRecord;
        }
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {
      PDLOG(DEBUG, "end of file %d, header size %d data length %d", buffer_.size(), kHeaderSize,
              length);
      return kWaitRecord;
    }
    // Check crc
    if (checksum_) {
      uint32_t expected_crc = Unmask(DecodeFixed32(header));
      uint32_t actual_crc = Value(header + 6, 1 + length);
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

    buffer_.remove_prefix(kHeaderSize + length);
    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      PDLOG(WARNING, "bad record with initial_offset");
      return kBadRecord;
    }
    *result = Slice(header + kHeaderSize, length);
    return type;
}

LogReader::LogReader(LogParts* logs, const std::string& log_path) : log_path_(log_path) {
    sf_ = NULL;
    reader_ = NULL;
    logs_ = logs;
    log_part_index_ = -1;
    start_offset_ = 0;
} 

LogReader::~LogReader() {
    delete sf_;
    delete reader_;
}

void LogReader::SetOffset(uint64_t start_offset) {
    start_offset_ = start_offset;
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

int LogReader::GetLogIndex() {
    return log_part_index_;
}

uint64_t LogReader::GetLastRecordEndOffset() {
    if (reader_ == NULL) {
        PDLOG(WARNING, "reader is NULL");
        return 0;
    }
    return reader_->LastRecordEndOffset();
}

::rtidb::base::Status LogReader::ReadNextRecord(
        ::rtidb::base::Slice* record, std::string* buffer) {
    // first read record 
    if (sf_ == NULL) {
         if (RollRLogFile() < 0) {
            PDLOG(WARNING, "fail to roll read log");
            return ::rtidb::base::Status::WaitRecord();
         }
    }
    ::rtidb::base::Status status = reader_->ReadRecord(record, buffer);
    if (status.IsEof()) {
        PDLOG(INFO, "reach the end of file. index %d", log_part_index_);
        if (RollRLogFile() < 0) {
            // reache the latest log part
            return status;
        }
        return ::rtidb::base::Status::Eof();
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
            PDLOG(DEBUG, "log index[%u] and start offset %lld", it->GetKey(),
                    it->GetValue());
            if (it->GetValue() <= start_offset_) {
                break;
            }
            it->Next();
        }
        if (it->Valid()) {
            index = (int)it->GetKey();
        } else {
            PDLOG(WARNING, "no log part matched! start_offset[%lu]", start_offset_); 
        }
    } else {
        uint32_t log_part_index = (uint32_t)log_part_index_;
        while (it->Valid()) {
            PDLOG(DEBUG, "log index[%u] and start offset %lu", it->GetKey(), it->GetValue());
            // find the next of current index log file part
            if (it->GetKey() == log_part_index + 1) {
                index = (int)it->GetKey();
                break;
            } else if (it->GetKey() == log_part_index) {
                PDLOG(DEBUG, "no new file. log_part_index %d", log_part_index);
                break;
            }
            uint32_t cur_index = it->GetKey();
            it->Next();
            if (it->Valid() && it->GetKey() <= log_part_index && cur_index > log_part_index) {
                index = (int)cur_index;
                break;
            }
        }
    }
    delete it;
    if (index >= 0) {
        // open a new log part file
        std::string full_path = log_path_ + "/" + 
            ::rtidb::base::FormatToString(index, FLAGS_binlog_name_length) + ".log";
        if (OpenSeqFile(full_path) != 0) {
            return -1;
        }
        delete reader_;
        // roll a new log part file, reset status
        reader_ = new Reader(sf_, NULL, FLAGS_binlog_enable_crc, 0);
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
    sf_ = ::rtidb::log::NewSeqFile(path, fd);
    return 0;
}


}  // namespace log
}  // namespace leveldb
