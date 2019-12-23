//
// segment_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "gtest/gtest.h"
#include "logging.h"
#include "base/file_util.h"
#include "storage/binlog.h"
#include "storage/mem_table.h"
#include "storage/mem_table_snapshot.h"
#include "storage/disk_table_snapshot.h"
#include "storage/ticket.h"
#include "proto/tablet.pb.h"
#include "log/log_writer.h"
#include "gflags/gflags.h"
#include <iostream>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <time.h>
#include "base/strings.h"
#include "base/file_util.h"
#include "timer.h"
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

DECLARE_string(db_root_path);
DECLARE_string(hdd_root_path);

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace log {
class WritableFile;
}
namespace storage {

const static ::rtidb::base::DefaultComparator scmp;

class SnapshotTest : public ::testing::Test {

public:
    SnapshotTest(){}
    ~SnapshotTest() {}
};

inline uint32_t GenRand() {
    return rand() % 10000000 + 1;
}

void RemoveData(const std::string& path) {
    ::rtidb::base::RemoveDir(path+"/data");
    ::rtidb::base::RemoveDir(path);
    ::rtidb::base::RemoveDir(FLAGS_hdd_root_path);
}

int GetManifest(const std::string file, ::rtidb::api::Manifest* manifest) {
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    google::protobuf::TextFormat::Parse(&fileInput, manifest);
    return 0;
}

bool RollWLogFile(WriteHandle** wh, LogParts* logs, const std::string& log_path, 
            uint32_t& binlog_index, uint64_t offset, bool append_end = true) {
    if (*wh != NULL) {
        if (append_end) {
            (*wh)->EndLog();
        }
        delete *wh;
        *wh = NULL;
    }
    std::string name = ::rtidb::base::FormatToString(binlog_index, 8) + ".log";
    ::rtidb::base::MkdirRecur(log_path);
    std::string full_path = log_path + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    logs->Insert(binlog_index, offset);
    *wh = new WriteHandle(name, fd);
    binlog_index++;
    return true;
}

TEST_F(SnapshotTest, Recover_binlog_and_snapshot) {
    std::string snapshot_dir = FLAGS_db_root_path + "/4_3/snapshot/";
    std::string binlog_dir = FLAGS_db_root_path + "/4_3/binlog/";
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    int count = 0;
    for (; count < 10; count++) {
        offset++;
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    MemTableSnapshot snapshot(4, 3, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>("test", 4, 3, 8, mapping, 0, ::rtidb::api::TTLType::kAbsoluteTime);
    table->Init();
    uint64_t offset_value = 0;;
    int ret = snapshot.MakeSnapshot(table, offset_value);
    ASSERT_EQ(0, ret); 
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    for (; count < 20; count++) {
        offset++;
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key2";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    for (; count < 30; count++) {
        offset++;
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + std::to_string(count);
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
        if (count == 25) {
            offset++;
            ::rtidb::api::LogEntry entry1;
            entry1.set_log_index(offset);
            entry1.set_method_type(::rtidb::api::MethodType::kDelete);
            ::rtidb::api::Dimension* dimension = entry1.add_dimensions();
            dimension->set_key(key);
            dimension->set_idx(0);
            entry1.set_term(5);
            std::string buffer1;
            entry1.SerializeToString(&buffer1);
            ::rtidb::base::Slice slice1(buffer1);
            ::rtidb::base::Status status = wh->Write(slice1);
        }
    }
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(31, latest_offset);
    Ticket ticket;
    TableIterator* it = table->NewIterator("key", ticket);
    it->Seek(1);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value1", value2_str);
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(0, it->GetKey());
    std::string value3_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value0", value3_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    it = table->NewIterator("key2", ticket);
    it->Seek(11);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(11, it->GetKey());
    std::string value4_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value11", value4_str);
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(10, it->GetKey());
    std::string value5_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value10", value5_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    it = table->NewIterator("key23", ticket);
    it->Seek(23);
    ASSERT_TRUE(it->Valid());
    delete it;
    it = table->NewIterator("key25", ticket);
    it->Seek(25);
    ASSERT_FALSE(it->Valid());
    delete it;
}

TEST_F(SnapshotTest, Recover_only_binlog_multi) {
    std::string snapshot_dir = FLAGS_db_root_path + "/4_4/snapshot/";
    std::string binlog_dir = FLAGS_db_root_path + "/4_4/binlog/";
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    int count = 0;
    for (; count < 10; count++) {
        offset++;
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_ts(count);
        entry.set_value("value" + std::to_string(count));
        ::rtidb::api::Dimension* d1 = entry.add_dimensions();
        d1->set_key("card0"); 
        d1->set_idx(0);
        ::rtidb::api::Dimension* d2 = entry.add_dimensions();
        d2->set_key("merchant0"); 
        d2->set_idx(1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("card", 0));
    mapping.insert(std::make_pair("merchant", 1));
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>("test", 4, 4, 8, mapping, 0, ::rtidb::api::TTLType::kAbsoluteTime);
    table->Init();
    MemTableSnapshot snapshot(4, 4, log_part, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(10, latest_offset);

    {
        Ticket ticket;
        TableIterator* it = table->NewIterator(0, "card0", ticket);
        it->Seek(1);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(1, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("value1", value2_str);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(0, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("value0", value3_str);
        it->Next();
        ASSERT_FALSE(it->Valid());
    }

    {
        Ticket ticket;
        TableIterator* it = table->NewIterator(1, "merchant0", ticket);
        it->Seek(1);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(1, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("value1", value2_str);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(0, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("value0", value3_str);
        it->Next();
        ASSERT_FALSE(it->Valid());
    }
}


TEST_F(SnapshotTest, Recover_only_binlog) {
    std::string snapshot_dir = FLAGS_db_root_path + "/3_3/snapshot/";
    std::string binlog_dir = FLAGS_db_root_path + "/3_3/binlog/";
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    int count = 0;
    for (; count < 10; count++) {
        offset++;
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>("test", 3, 3, 8, mapping, 0, ::rtidb::api::TTLType::kAbsoluteTime);
    table->Init();
    MemTableSnapshot snapshot(3, 3, log_part, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(10, latest_offset);
    Ticket ticket;
    TableIterator* it = table->NewIterator("key", ticket);
    it->Seek(1);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value1", value2_str);
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(0, it->GetKey());
    std::string value3_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value0", value3_str);
    it->Next();
    ASSERT_FALSE(it->Valid());

}

TEST_F(SnapshotTest, Recover_only_snapshot_multi) {
    std::string snapshot_dir = FLAGS_db_root_path + "/3_2/snapshot";
    std::string binlog_dir = FLAGS_db_root_path + "/3_2/binlog";

    ::rtidb::base::MkdirRecur(snapshot_dir);
    {
        std::string snapshot1 = "20170609.sdb";
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::rtidb::log::WritableFile* wf = ::rtidb::log::NewWritableFile(snapshot1, fd_w);
        ::rtidb::log::Writer writer(wf);
        {
            ::rtidb::api::LogEntry entry;
            entry.set_ts(9527);
            entry.set_value("test1");
            entry.set_log_index(1);
            ::rtidb::api::Dimension* d1 = entry.add_dimensions();
            d1->set_key("card0");
            d1->set_idx(0);
            ::rtidb::api::Dimension* d2 = entry.add_dimensions();
            d2->set_key("merchant0");
            d2->set_idx(1);
            std::string val;
            bool ok = entry.SerializeToString(&val);
            ASSERT_TRUE(ok);
            Slice sval(val.c_str(), val.size());
            Status status = writer.AddRecord(sval);
            ASSERT_TRUE(status.ok());

        }
        {
            ::rtidb::api::LogEntry entry;
            entry.set_ts(9528);
            entry.set_value("test2");
            entry.set_log_index(2);
            ::rtidb::api::Dimension* d1 = entry.add_dimensions();
            d1->set_key("card0");
            d1->set_idx(0);
            ::rtidb::api::Dimension* d2 = entry.add_dimensions();
            d2->set_key("merchant0");
            d2->set_idx(1);
            std::string val;
            bool ok = entry.SerializeToString(&val);
            ASSERT_TRUE(ok);
            Slice sval(val.c_str(), val.size());
            Status status = writer.AddRecord(sval);
            ASSERT_TRUE(status.ok());
        }
    }

    {
        std::string snapshot1 = "20170610.sdb.tmp";
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::rtidb::log::WritableFile* wf = ::rtidb::log::NewWritableFile(snapshot1, fd_w);
        ::rtidb::log::Writer writer(wf);
        ::rtidb::api::LogEntry entry;
        entry.set_pk("test1");
        entry.set_ts(9527);
        entry.set_value("test1");
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry.set_pk("test1");
        entry.set_ts(9528);
        entry.set_value("test2");
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());
    }

    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("card", 0));
    mapping.insert(std::make_pair("merchant", 1));
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>("test", 3, 2, 8, mapping, 0, ::rtidb::api::TTLType::kAbsoluteTime);
    table->Init();
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(3, 2, log_part, FLAGS_db_root_path);
    ASSERT_TRUE(snapshot.Init());
    int ret = snapshot.GenManifest("20170609.sdb", 3, 2, 5);
    ASSERT_EQ(0, ret);
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(2, latest_offset);
    {
        Ticket ticket;
        TableIterator* it = table->NewIterator(0, "card0", ticket);
        it->Seek(9528);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9528, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("test2", value2_str);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9527, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("test1", value3_str);
        it->Next();
        ASSERT_FALSE(it->Valid());
    }
    {
        Ticket ticket;
        TableIterator* it = table->NewIterator(1, "merchant0", ticket);
        it->Seek(9528);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9528, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("test2", value2_str);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9527, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("test1", value3_str);
        it->Next();
        ASSERT_FALSE(it->Valid());
    }
    ASSERT_EQ(2, table->GetRecordCnt());
    ASSERT_EQ(4, table->GetRecordIdxCnt());

}


TEST_F(SnapshotTest, Recover_only_snapshot) {
    std::string snapshot_dir = FLAGS_db_root_path + "/2_2/snapshot";

    ::rtidb::base::MkdirRecur(snapshot_dir);
    {
        std::string snapshot1 = "20170609.sdb";
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::rtidb::log::WritableFile* wf = ::rtidb::log::NewWritableFile(snapshot1, fd_w);
        ::rtidb::log::Writer writer(wf);
        ::rtidb::api::LogEntry entry;
        entry.set_pk("test0");
        entry.set_ts(9527);
        entry.set_value("test1");
        entry.set_log_index(1);
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry.set_pk("test0");
        entry.set_ts(9528);
        entry.set_value("test2");
        entry.set_log_index(2);
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());

    }

    {
        std::string snapshot1 = "20170610.sdb.tmp";
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::rtidb::log::WritableFile* wf = ::rtidb::log::NewWritableFile(snapshot1, fd_w);
        ::rtidb::log::Writer writer(wf);
        ::rtidb::api::LogEntry entry;
        entry.set_pk("test1");
        entry.set_ts(9527);
        entry.set_value("test1");
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry.set_pk("test1");
        entry.set_ts(9528);
        entry.set_value("test2");
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());
    }

    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));

    std::shared_ptr<MemTable> table = std::make_shared<MemTable>("test", 2, 2, 8, mapping, 0, ::rtidb::api::TTLType::kAbsoluteTime);
    table->Init();
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(2, 2, log_part, FLAGS_db_root_path);
    ASSERT_TRUE(snapshot.Init());
    int ret = snapshot.GenManifest("20170609.sdb", 2, 2, 5);
    ASSERT_EQ(0, ret);
    uint64_t offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, offset));
    ASSERT_EQ(2, offset);
    Ticket ticket;
    TableIterator* it = table->NewIterator("test0", ticket);
    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9528, it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test2", value2_str);
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9527, it->GetKey());
    std::string value3_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test1", value3_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_F(SnapshotTest, MakeSnapshot) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(1, 2, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>("tx_log", 1, 1, 8, mapping, 2, ::rtidb::api::TTLType::kAbsoluteTime);
    table->Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    std::string log_path = FLAGS_db_root_path + "/1_2/binlog/";
    std::string snapshot_path = FLAGS_db_root_path + "/1_2/snapshot/";
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset++);
    int count = 0;
    for (; count < 10; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + std::to_string(count);
        entry.set_pk(key);
        entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        entry.set_value("value");
        entry.set_term(5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        offset++;
        if (count % 2 == 0) {
            ::rtidb::api::LogEntry entry1;
            entry1.set_log_index(offset);
            entry1.set_method_type(::rtidb::api::MethodType::kDelete);
            ::rtidb::api::Dimension* dimension = entry1.add_dimensions();
            dimension->set_key(key);
            dimension->set_idx(0);
            entry1.set_term(5);
            std::string buffer1;
            entry1.SerializeToString(&buffer1);
            ::rtidb::base::Slice slice1(buffer1);
            ::rtidb::base::Status status = wh->Write(slice1);
            offset++;
        }
        if (count % 4 == 0) {
            entry.set_log_index(offset);
            std::string buffer2;
            entry.SerializeToString(&buffer2);
            ::rtidb::base::Slice slice2(buffer2);
            ::rtidb::base::Status status = wh->Write(slice2);
            offset++;
        }
    }
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    for (; count < 30; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + std::to_string(count);
        entry.set_pk(key);
        entry.set_term(6);
        if (count == 20) {
            // set one timeout key
            entry.set_ts(::baidu::common::timer::get_micros() / 1000 - 4 * 60 * 1000);
        } else {
            entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        }
        entry.set_value("value");
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        offset++;
    }
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::rtidb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, vec.size());
    vec.clear();
    ret = ::rtidb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, vec.size());

    std::string full_path = snapshot_path + "MANIFEST";
    ::rtidb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(38, manifest.offset());
    ASSERT_EQ(27, manifest.count());
    ASSERT_EQ(6, manifest.term());

    for (; count < 50; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + std::to_string(count);
        entry.set_pk(key);
        entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        entry.set_value("value");
        entry.set_term(7);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        offset++;
    }
    {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_method_type(::rtidb::api::MethodType::kDelete);
        ::rtidb::api::Dimension* dimension = entry.add_dimensions();
        std::string key = "key9";
        dimension->set_key(key);
        dimension->set_idx(0);
        entry.set_term(5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        offset++;
    }

    ret = snapshot.MakeSnapshot(table, offset_value);
    ASSERT_EQ(0, ret);
    vec.clear();
    ret = ::rtidb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, vec.size());
    vec.clear();
    ret = ::rtidb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, vec.size());
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }

    ASSERT_EQ(59, manifest.offset());
    ASSERT_EQ(46, manifest.count());
    ASSERT_EQ(7, manifest.term());
}

TEST_F(SnapshotTest, MakeSnapshotLatest) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(5, 1, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>("tx_log", 5, 1, 8, mapping, 4, ::rtidb::api::TTLType::kLatestTime);
    table->Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    std::string log_path = FLAGS_db_root_path + "/5_1/binlog/";
    std::string snapshot_path = FLAGS_db_root_path + "/5_1/snapshot/";
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    int count = 0;
    for (; count < 10; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + std::to_string(count % 4);
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value");
        entry.set_term(5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        table->Put(key, count, "value", 5);
        offset++;
    }
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    for (; count < 30; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + std::to_string(count % 4);
        entry.set_pk(key);
        entry.set_term(6);
        entry.set_ts(count);
        entry.set_value("value");
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        table->Put(key, count, "value", 5);
        offset++;
    }
    table->SchedGc();
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::rtidb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, vec.size());
    vec.clear();
    ret = ::rtidb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, vec.size());

    std::string full_path = snapshot_path + "MANIFEST";
    ::rtidb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(offset-1, manifest.offset());
    ASSERT_EQ(16, manifest.count());
    ASSERT_EQ(6, manifest.term());

    for (; count < 1000; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key1000";
        if (count == 100) {
            key = "key2222";
        }
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value");
        entry.set_term(7);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        table->Put(key, count, "value", 5);
        offset++;
    }
    table->SchedGc();
    ret = snapshot.MakeSnapshot(table, offset_value);
    ASSERT_EQ(0, ret);
    vec.clear();
    ret = ::rtidb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, vec.size());
    vec.clear();
    ret = ::rtidb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, vec.size());
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }

    ASSERT_EQ(offset-1, manifest.offset());
    ASSERT_EQ(21, manifest.count());
    ASSERT_EQ(7, manifest.term());
}

TEST_F(SnapshotTest, RecordOffset) {
	std::string snapshot_path = FLAGS_db_root_path + "/1_1/snapshot/";
    MemTableSnapshot snapshot(1, 1, NULL, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t offset = 1122;
    uint64_t key_count = 3000;
    uint64_t term = 0;
    std::string snapshot_name = ::rtidb::base::GetNowTime() + ".sdb";
    int ret = snapshot.GenManifest(snapshot_name, key_count, offset, term);
    ASSERT_EQ(0, ret);
    std::string value;
    ::rtidb::api::Manifest manifest;
    GetManifest(snapshot_path + "MANIFEST", &manifest);
    ASSERT_EQ(offset, manifest.offset());
    ASSERT_EQ(term, manifest.term());
    sleep(1);

    std::string snapshot_name1 = ::rtidb::base::GetNowTime() + ".sdb";
    uint64_t key_count1 = 3001;
    offset = 1124;
    term = 10;
    ret = snapshot.GenManifest(snapshot_name1, key_count1, offset, term);
    ASSERT_EQ(0, ret);
    GetManifest(snapshot_path + "MANIFEST", &manifest);
    ASSERT_EQ(offset, manifest.offset());
    ASSERT_EQ(key_count1, manifest.count());
    ASSERT_EQ(term, manifest.term());
}

TEST_F(SnapshotTest, Recover_empty_binlog) {
    uint32_t tid = GenRand();
    std::string snapshot_dir = FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/snapshot/";
    std::string binlog_dir = FLAGS_db_root_path + "/" + std::to_string(tid) + "_0/binlog/";
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    int count = 0;
    for (; count < 10; count++) {
        offset++;
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    // not set end falg
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    // no record binlog
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    // empty binlog
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    count = 0;
    for (; count < 10; count++) {
        offset++;
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key_new";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value_new" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    binlog_index++;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    count = 0;
    for (; count < 10; count++) {
        offset++;
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key_xxx";
        entry.set_pk(key);
        entry.set_ts(count);
        entry.set_value("value_xxx" + std::to_string(count));
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    delete wh;

    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>("test", tid, 0, 8, mapping, 0, ::rtidb::api::TTLType::kAbsoluteTime);
    table->Init();
    MemTableSnapshot snapshot(tid, 0, log_part, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(30, latest_offset);
    Ticket ticket;
    TableIterator* it = table->NewIterator("key_new", ticket);
    it->Seek(1);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value_new1", value2_str);
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(0, it->GetKey());
    std::string value3_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value_new0", value3_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    it = table->NewIterator("key_xxx", ticket);
    it->Seek(1);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    std::string value4_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value_xxx1", value4_str);

    // check snapshot
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::rtidb::base::GetFileName(snapshot_dir, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, vec.size());
    vec.clear();
    std::string full_path = snapshot_dir + "MANIFEST";
    ::rtidb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(30, manifest.offset());
    ASSERT_EQ(30, manifest.count());
}

TEST_F(SnapshotTest, Recover_snapshot_ts) {
    std::string snapshot_dir = FLAGS_db_root_path + "/2_2/snapshot";
    ::rtidb::base::MkdirRecur(snapshot_dir);
    {
        std::string snapshot1 = "20190614.sdb";
        std::string full_path = snapshot_dir + "/" + snapshot1;
        printf("path:%s\n", full_path.c_str());
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::rtidb::log::WritableFile* wf = ::rtidb::log::NewWritableFile(snapshot1, fd_w);
        ::rtidb::log::Writer writer(wf);
        ::rtidb::api::LogEntry entry;
        entry.set_pk("test0");
        entry.set_ts(9527);
        entry.set_value("value0");
        entry.set_log_index(1);
        ::rtidb::api::Dimension* dim = entry.add_dimensions();
        dim->set_key("card0");
        dim->set_idx(0);
        ::rtidb::api::Dimension* dim1 = entry.add_dimensions();
        dim1->set_key("mcc0");
        dim1->set_idx(1);
        ::rtidb::api::TSDimension* ts_dim = entry.add_ts_dimensions();
        ts_dim->set_ts(1122);
        ts_dim->set_idx(0);
        ::rtidb::api::TSDimension* ts_dim1 = entry.add_ts_dimensions();
        ts_dim1->set_ts(2233);
        ts_dim1->set_idx(1);
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
    }
    
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_name("test");
    table_meta.set_tid(2);
    table_meta.set_pid(2);
    table_meta.set_ttl(0);
    table_meta.set_seg_cnt(8);
    ::rtidb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("card");
    column_desc1->set_type("string");
    ::rtidb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("mcc");
    column_desc2->set_type("string");
    ::rtidb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("amt");
    column_desc3->set_type("double");
    ::rtidb::common::ColumnDesc* column_desc4 = table_meta.add_column_desc();
    column_desc4->set_name("ts1");
    column_desc4->set_type("int64");
    column_desc4->set_is_ts_col(true);
    ::rtidb::common::ColumnDesc* column_desc5 = table_meta.add_column_desc();
    column_desc5->set_name("ts2");
    column_desc5->set_type("int64");
    column_desc5->set_is_ts_col(true);
    ::rtidb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("card");
    column_key1->add_ts_name("ts1");
    column_key1->add_ts_name("ts2");
    ::rtidb::common::ColumnKey* column_key2 = table_meta.add_column_key();
    column_key2->set_index_name("mcc");
    column_key2->add_ts_name("ts1");
    table_meta.set_mode(::rtidb::api::TableMode::kTableLeader);
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>(table_meta);
    table->Init();
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(2, 2, log_part, FLAGS_db_root_path);
    ASSERT_TRUE(snapshot.Init());
    int ret = snapshot.GenManifest("20190614.sdb", 1, 1, 5);
    ASSERT_EQ(0, ret);
    uint64_t offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, offset));
    ASSERT_EQ(1, offset);
    Ticket ticket;
    TableIterator* it = table->NewIterator(0, 0, "card0", ticket);
    it->Seek(1122);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1122, it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value0", value2_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    it = table->NewIterator(0, 1, "card0", ticket);
    it->Seek(2233);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2233, it->GetKey());
    value2_str.assign(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value0", value2_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    it = table->NewIterator(1, 0, "mcc0", ticket);
    it->Seek(1122);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1122, it->GetKey());
    value2_str.assign(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value0", value2_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
    it = table->NewIterator(0, 0, "mcc0", ticket);
    it->Seek(1122);
    ASSERT_FALSE(it->Valid());
    delete it;
}

TEST_F(SnapshotTest, DiskTableMakeSnapshot) {
    std::string snapshot_path = FLAGS_hdd_root_path + "/1_1/snapshot/";
    DiskTableSnapshot snapshot(1, 1, ::rtidb::common::StorageMode::kHDD, FLAGS_hdd_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* disk_table_ptr = new DiskTable("test", 1, 1, mapping, 0, 
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    std::shared_ptr<Table> table(disk_table_ptr);
    table->Init();
    
    for (int idx = 0; idx < 1000; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    
    snapshot.SetTerm(9);
    uint64_t offset = 0;
    int ret = snapshot.MakeSnapshot(table, offset);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(10000, offset);

    std::string full_path = snapshot_path + "MANIFEST";
    ::rtidb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(10000, manifest.offset());
    ASSERT_EQ(10000, manifest.count());
    ASSERT_EQ(9, manifest.term());
    
    std::string path = FLAGS_hdd_root_path + "/1_1";
    RemoveData(path);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    FLAGS_db_root_path = "/tmp/" + std::to_string(::rtidb::storage::GenRand());
    FLAGS_hdd_root_path = "/tmp/" + std::to_string(::rtidb::storage::GenRand());
    return RUN_ALL_TESTS();
}
