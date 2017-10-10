//
// log_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//

#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <boost/atomic.hpp>
#include "base/file_util.h"
#include <iostream>
#include "log/crc32c.h"
#include "log/coding.h"
#include <vector>

using ::rtidb::base::Slice;
using ::rtidb::base::Status;

namespace rtidb {
namespace log {

class LogWRTest : public ::testing::Test {

public:
    LogWRTest() {}
    ~LogWRTest () {}
};

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
}

uint32_t type_crc_[kMaxRecordType + 1];
int block_offset_ = 0;

void InitTypeCrc(uint32_t* type_crc) {
    for (int i = 0; i <= kMaxRecordType; i++) {
        char t = static_cast<char>(i);
        type_crc[i] = Value(&t, 1);
    }
}

void GenPhysicalRecord(RecordType t, const char* ptr, size_t n, int& block_offset_, std::vector<std::string>& rec_vec) {
    assert(n <= 0xffff);  // Must fit in two bytes
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);
    // Format the header
    char buf[kHeaderSize];
    buf[4] = static_cast<char>(n & 0xff);
    buf[5] = static_cast<char>(n >> 8);
    buf[6] = static_cast<char>(t);

    // Compute the crc of the record type and the payload.
    uint32_t crc = Extend(type_crc_[t], ptr, n);
    crc = Mask(crc);                 // Adjust for storage
    EncodeFixed32(buf, crc);

    std::string str(buf, kHeaderSize);
    str.append(ptr, n);
    rec_vec.push_back(str);

    block_offset_ += kHeaderSize + n;
}

int AddRecord(const Slice& slice, std::vector<std::string>& vec) {
    const char* ptr = slice.data();
    size_t left = slice.size();

    InitTypeCrc(type_crc_);

    // Fragment the record if necessary and emit it.  Note that if slice
    // is empty, we still want to iterate once to emit a single
    // zero-length record
    Status s;
    bool begin = true;
    do {
        const int leftover = kBlockSize - block_offset_;
        assert(leftover >= 0);
        if (leftover < kHeaderSize) {
          // Switch to a new block
            if (leftover > 0) {
                // Fill the trailer (literal below relies on kHeaderSize being 7)
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

TEST_F(LogWRTest, TestWriteAndRead) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::rtidb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(wf);
    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    Reader reader(rf, NULL, true, 0);
    Status status = writer.AddRecord("hello");
    ASSERT_TRUE(status.ok());
    std::string scratch;
    Slice value;
    status = reader.ReadRecord(&value, &scratch);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ("hello", value.ToString());
    uint64_t last_record_offset = reader.LastRecordOffset();
    std::cout << "last record offset " << last_record_offset << std::endl;
    status = writer.AddRecord("hello1");
    ASSERT_TRUE(status.ok());
    Reader reader2(rf, NULL, true, last_record_offset);
    std::string scratch2;
    Slice value2;
    status = reader2.ReadRecord(&value2, &scratch2);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ("hello1", value2.ToString());
}

TEST_F(LogWRTest, TestLogEntry) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::rtidb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(wf);

    ::rtidb::api::LogEntry entry;
    entry.set_pk("test0");
    entry.set_ts(9527);
    entry.set_value("test1");
    std::string val;
    bool ok = entry.SerializeToString(&val);
    ASSERT_TRUE(ok);
    Slice sval(val.c_str(), val.size());
    Status status = writer.AddRecord(sval);
    ASSERT_TRUE(status.ok());
    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    std::string scratch2;
    Reader reader(rf, NULL, true, 0);
    {
        Slice value2;
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.ok());
        ::rtidb::api::LogEntry entry2;
        ok = entry2.ParseFromString(value2.ToString());
        ASSERT_TRUE(ok);
        ASSERT_EQ("test0", entry2.pk());
        ASSERT_EQ("test1", entry2.value());
        ASSERT_EQ(9527, entry2.ts());
    }
    status = writer.AddRecord(sval);
    ASSERT_TRUE(status.ok());
    {
        Slice value2;
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.ok());
        ::rtidb::api::LogEntry entry2;
        ok = entry2.ParseFromString(value2.ToString());
        ASSERT_TRUE(ok);
        ASSERT_EQ("test0", entry2.pk());
        ASSERT_EQ("test1", entry2.value());
        ASSERT_EQ(9527, entry2.ts());
        status = reader.ReadRecord(&value2, &scratch2);
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(status.IsWaitRecord());
        status = reader.ReadRecord(&value2, &scratch2);
        ASSERT_TRUE(status.ok());
        ok = entry2.ParseFromString(value2.ToString());
        ASSERT_TRUE(ok);
        ASSERT_EQ("test0", entry2.pk());
        ASSERT_EQ("test1", entry2.value());
        ASSERT_EQ(9527, entry2.ts());
    }
}

TEST_F(LogWRTest, TestWait) {
    std::string log_dir = "/tmp/" + GenRand() + "/";
    ::rtidb::base::MkdirRecur(log_dir);
    std::string fname = "test.log";
    std::string full_path = log_dir + "/" + fname;
    FILE* fd_w = fopen(full_path.c_str(), "ab+");
    ASSERT_TRUE(fd_w != NULL);
    WritableFile* wf = NewWritableFile(fname, fd_w);
    Writer writer(wf);
    std::string val(1024 * 5, 'a');
    Slice sval(val.c_str(), val.size());
    Status status = writer.AddRecord(sval);
    ASSERT_TRUE(status.ok());

    block_offset_ = 1024 * 5 + kHeaderSize * 2  - kBlockSize;

    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    std::string scratch2;
    Reader reader(rf, NULL, true, 0);
    Slice value;
    status = reader.ReadRecord(&value, &scratch2);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1024 * 5, value.size());
    value.clear();
    scratch2.clear();

    // write partial record less than kHeadsize
    std::string val3(1024 * 5, 'c');
    Slice sval3(val3.c_str(), val3.size());
    std::vector<std::string> vec;
    AddRecord(sval3, vec);
    ASSERT_EQ(2, vec.size());

    status = wf->Append(Slice(vec[0].c_str(), vec[0].length()));
    ASSERT_TRUE(status.ok());
    wf->Flush();
    status = reader.ReadRecord(&value, &scratch2);
    ASSERT_TRUE(status.IsWaitRecord());


    status = wf->Append(Slice(vec[1].c_str(), vec[1].length()));
    wf->Flush();
    status = reader.ReadRecord(&value, &scratch2);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1024 * 5, value.size());
}
/*TEST_F(LogWRTest, ReadAllLogs) {
    std::string log_dir = "/tmp/";
    ::rtidb::base::MkdirRecur(log_dir);
    std::string fname = "00000000.log";
    std::string full_path = log_dir + "/" + fname;
    FILE* fd_r = fopen(full_path.c_str(), "rb");
    ASSERT_TRUE(fd_r != NULL);
    SequentialFile* rf = NewSeqFile(fname, fd_r);
    Reader reader(rf, NULL, true, 0);
    Slice value2;
    std::string scratch2;
    while (true) {
        Status status = reader.ReadRecord(&value2, &scratch2);
        if (status.ok()) {
            ::rtidb::api::LogEntry entry2;
            bool ok = entry2.ParseFromString(value2.ToString());
            if (ok) {
                std::cout << entry2.log_index() << std::endl;
            }
        }
        if (status.IsEof()) {
            return;
        }
    }
    
}*/




}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




