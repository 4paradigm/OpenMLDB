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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <vector>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "config.h"  // NOLINT
#include "log/coding.h"
#include "log/crc32c.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"

using ::openmldb::base::Slice;
using ::openmldb::log::Status;

DECLARE_string(snapshot_compression);
bool compressed_ = true;
uint32_t block_size_ = 1024 * 4;
uint32_t header_size_ = 7;

namespace openmldb {
namespace log {

class LogWRTest : public ::testing::Test {
 public:
    LogWRTest() {}
    ~LogWRTest() {}
};

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);  // NOLINT
}  // NOLINT

uint32_t type_crc_[kMaxRecordType + 1];
int block_offset_ = 0;

void InitTypeCrc(uint32_t* type_crc) {
    for (int i = 0; i <= kMaxRecordType; i++) {
        char t = static_cast<char>(i);
        type_crc[i] = Value(&t, 1);
    }
}

void GenPhysicalRecord(RecordType t, const char* ptr, size_t n,
                       int& block_offset_,                   // NOLINT
                       std::vector<std::string>& rec_vec) {  // NOLINT
    assert(n <= 0xffff);                                     // Must fit in two bytes
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);
    // Format the header
    char buf[kHeaderSize];
    buf[4] = static_cast<char>(n & 0xff);
    buf[5] = static_cast<char>(n >> 8);
    buf[6] = static_cast<char>(t);

    // Compute the crc of the record type and the payload.
    uint32_t crc = Extend(type_crc_[t], ptr, n);
    crc = Mask(crc);  // Adjust for storage
    EncodeFixed32(buf, crc);

    std::string str(buf, kHeaderSize);
    str.append(ptr, n);
    rec_vec.push_back(str);

    block_offset_ += kHeaderSize + n;
}

int AddRecord(const Slice& slice, std::vector<std::string>& vec) {  // NOLINT
    const char* ptr = slice.data();
    size_t left = slice.size();

    InitTypeCrc(type_crc_);

    // Fragment the record if necessary and emit it.  Note that if slice
    // is empty, we still want to iterate once to emit a single
    // zero-length record
    Status s;
    bool begin = true;
    do {
        const int32_t leftover = kBlockSize - block_offset_;
        assert(leftover >= 0);
        if (leftover < static_cast<int32_t>(kHeaderSize)) {
            // Switch to a new block
            if (leftover > 0) {
                // Fill the trailer (literal below relies on kHeaderSize being
                // 7)
                assert(kHeaderSize == 7);
                vec.push_back(std::string("\x00\x00\x00\x00\x00\x00"));
            }
            block_offset_ = 0;
        }
        // Invariant: we never leave < kHeaderSize bytes in a block.
        assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

        const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
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
        GenPhysicalRecord(type, ptr, fragment_length, block_offset_, vec);
        ptr += fragment_length;
        left -= fragment_length;
        begin = false;
    } while (left > 0);
    return 0;
}

std::string GetWritePath(const std::string& path) {
    if (FLAGS_snapshot_compression == "zlib") {
        return path + openmldb::log::ZLIB_COMPRESS_SUFFIX;
    } else if (FLAGS_snapshot_compression == "snappy") {
        return path + openmldb::log::SNAPPY_COMPRESS_SUFFIX;
    } else {
        return path;
    }
}

TEST_F(LogWRTest, TestWriteSize) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::openmldb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + GetWritePath(fname);
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    wf->Append(Slice("1234567"));
    wf->Append(Slice("abcde"));
    ASSERT_EQ(12u, wf->GetSize());
    delete wf;
}

TEST_F(LogWRTest, TestWriteAndRead) {
    std::vector<std::string> val_vec1{"hello", std::string(block_size_ - header_size_, 'a')};
    std::vector<std::string> val_vec2{"hello1", std::string(block_size_ - header_size_, 'b')};
    for (int i = 0; i < 1; i++) {
        std::string log_dir = "/tmp/" + GenRand() + "/";
        ::openmldb::base::MkdirRecur(log_dir);
        std::string fname = "test.log";
        std::string full_path = log_dir + "/" + fname;
        full_path = GetWritePath(full_path);
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        WritableFile* wf = NewWritableFile(fname, fd_w);
        Writer writer(FLAGS_snapshot_compression, wf);
        FILE* fd_r = fopen(full_path.c_str(), "rb");
        ASSERT_TRUE(fd_r != NULL);
        SequentialFile* rf = NewSeqFile(fname, fd_r);
        Reader reader(rf, NULL, true, 0, compressed_);
        Status status = writer.AddRecord(val_vec1[i]);
        ASSERT_TRUE(status.ok());
        if (FLAGS_snapshot_compression != "off" && i == 0) {
            writer.EndLog();
        }
        std::string scratch;
        Slice value;
        status = reader.ReadRecord(&value, &scratch);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val_vec1[i], value.ToString());
        uint64_t last_record_offset = reader.LastRecordOffset();
        std::cout << "last record offset " << last_record_offset << std::endl;
        if (FLAGS_snapshot_compression != "off" && i == 0) {
            status = reader.ReadRecord(&value, &scratch);
            ASSERT_TRUE(status.IsEof());
        }
        status = writer.AddRecord(val_vec2[i]);
        ASSERT_TRUE(status.ok());
        if (FLAGS_snapshot_compression != "off" && i == 0) {
            writer.EndLog();
        }
        Reader reader2(rf, NULL, true, last_record_offset, compressed_);
        std::string scratch2;
        Slice value2;
        status = reader2.ReadRecord(&value2, &scratch2);
        std::cout << "status: " << status.ToString() << std::endl;
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val_vec2[i], value2.ToString());
    }
}

TEST_F(LogWRTest, TestLogEntry) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::openmldb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    full_path = GetWritePath(full_path);
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(FLAGS_snapshot_compression, wf);

    ::openmldb::api::LogEntry entry;
    entry.set_pk("test0");
    entry.set_ts(9527);
    entry.set_value("test1");
    std::string val;
    bool ok = entry.SerializeToString(&val);
    ASSERT_TRUE(ok);
    Slice sval(val.c_str(), val.size());
    Status status = writer.AddRecord(sval);
    ASSERT_TRUE(status.ok());
    if (FLAGS_snapshot_compression != "off") {
        writer.EndLog();
    }
    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    std::string scratch2;
    Reader reader(rf, NULL, true, 0, compressed_);
    {
        Slice value2;
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.ok());
        ::openmldb::api::LogEntry entry2;
        ok = entry2.ParseFromString(value2.ToString());
        ASSERT_TRUE(ok);
        ASSERT_EQ("test0", entry2.pk());
        ASSERT_EQ("test1", entry2.value());
        ASSERT_EQ(9527u, entry2.ts());
    }
    status = writer.AddRecord(sval);
    ASSERT_TRUE(status.ok());
    if (FLAGS_snapshot_compression != "off") {
        Slice value2;
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.IsEof());
        writer.EndLog();
    }
    {
        Slice value2;
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.ok());
        ::openmldb::api::LogEntry entry2;
        ok = entry2.ParseFromString(value2.ToString());
        ASSERT_TRUE(ok);
        ASSERT_EQ("test0", entry2.pk());
        ASSERT_EQ("test1", entry2.value());
        ASSERT_EQ(9527u, entry2.ts());
        if (FLAGS_snapshot_compression != "off") {
            Slice value2;
            status = reader.ReadRecord(&value2, &scratch2);
            ASSERT_TRUE(status.IsEof());
            return;
        }
        status = reader.ReadRecord(&value2, &scratch2);
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(status.IsWaitRecord());
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.ok());
        ok = entry2.ParseFromString(value2.ToString());
        ASSERT_TRUE(ok);
        ASSERT_EQ("test0", entry2.pk());
        ASSERT_EQ("test1", entry2.value());
        ASSERT_EQ(9527u, entry2.ts());
    }
}

TEST_F(LogWRTest, TestWait) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::openmldb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    full_path = GetWritePath(full_path);
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(FLAGS_snapshot_compression, wf);
    uint32_t value_size = 1024 * 5;
    if (FLAGS_snapshot_compression != "off") {
        value_size = 5 * 1024 * 1024;
    }
    std::string val(value_size, 'a');
    Slice sval(val.c_str(), val.size());
    Status status = writer.AddRecord(sval);
    ASSERT_TRUE(status.ok());
    if (FLAGS_snapshot_compression != "off") {
        writer.EndLog();
    }

    block_offset_ = value_size + header_size_ * 2 - block_size_;

    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    std::string scratch2;
    Reader reader(rf, NULL, true, 0, compressed_);
    Slice value;
    status = reader.ReadRecord(&value, &scratch2);
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value_size, value.size());
    value.clear();
    scratch2.clear();

    // write partial record less than kHeadsize
    if (FLAGS_snapshot_compression == "off") {
        std::string val3(value_size, 'c');
        Slice sval3(val3.c_str(), val3.size());
        std::vector<std::string> vec;
        AddRecord(sval3, vec);
        ASSERT_EQ(2u, vec.size());

        status = wf->Append(Slice(vec[0].c_str(), vec[0].length()));
        ASSERT_TRUE(status.ok());
        wf->Flush();
        status = reader.ReadRecord(&value, &scratch2);
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(status.IsWaitRecord());

        status = wf->Append(Slice(vec[1].c_str(), vec[1].length()));
        wf->Flush();
        status = reader.ReadRecord(&value, &scratch2);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value_size, value.size());
    }
}

TEST_F(LogWRTest, TestGoBack) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::openmldb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(FLAGS_snapshot_compression, wf);
    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    Reader reader(rf, NULL, true, 0, compressed_);
    Status status = writer.AddRecord("hello");
    ASSERT_TRUE(status.ok());
    if (FLAGS_snapshot_compression != "off") {
        writer.EndLog();
    }
    std::string scratch;
    Slice value;
    status = reader.ReadRecord(&value, &scratch);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ("hello", value.ToString());
    uint64_t last_record_offset = reader.LastRecordOffset();
    std::cout << "last record offset " << last_record_offset << std::endl;
    status = writer.AddRecord("hello1");
    ASSERT_TRUE(status.ok());
    if (FLAGS_snapshot_compression != "off") {
        status = reader.ReadRecord(&value, &scratch);
        ASSERT_TRUE(status.IsEof());
        writer.EndLog();
    }
    Slice value2;
    status = reader.ReadRecord(&value2, &scratch);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ("hello1", value2.ToString());
    reader.GoBackToStart();
    Slice value3;
    status = reader.ReadRecord(&value3, &scratch);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ("hello", value3.ToString());
}

TEST_F(LogWRTest, TestInit) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::openmldb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(FLAGS_snapshot_compression, wf);
    ASSERT_EQ(block_size_, writer.GetBlockSize());
    ASSERT_EQ(header_size_, writer.GetHeaderSize());
    ASSERT_EQ(writer.GetCompressType(FLAGS_snapshot_compression), writer.GetCompressType());

    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    Reader reader(rf, NULL, true, 0, compressed_);
    ASSERT_EQ(block_size_, reader.GetBlockSize());
    ASSERT_EQ(header_size_, reader.GetHeaderSize());
    ASSERT_EQ(compressed_, reader.GetCompressed());
}

}  // namespace log
}  // namespace openmldb

int main(int argc, char** argv) {
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    int ret = 0;
    std::vector<std::string> vec{"off", "zlib", "snappy"};
    for (size_t i = 0; i < vec.size(); i++) {
        std::cout << "compress type: " << vec[i] << std::endl;
        FLAGS_snapshot_compression = vec[i];
        if (FLAGS_snapshot_compression == "off") {
            compressed_ = false;
            block_size_ = openmldb::log::kBlockSize;
            header_size_ = openmldb::log::kHeaderSize;
        } else {
            block_size_ = openmldb::log::kCompressBlockSize;
            header_size_ = openmldb::log::kHeaderSizeForCompress;
            compressed_ = true;
        }
        ret += RUN_ALL_TESTS();
    }
    return ret;
    // return RUN_ALL_TESTS();
}
