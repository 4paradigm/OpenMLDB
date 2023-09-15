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
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <iostream>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "base/strings.h"
#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "log/log_writer.h"
#include "log/status.h"
#include "proto/tablet.pb.h"
#include "storage/binlog.h"
#include "storage/mem_table.h"
#include "storage/mem_table_snapshot.h"
#include "storage/ticket.h"
#include "test/util.h"

DECLARE_string(db_root_path);
DECLARE_string(snapshot_compression);

using ::openmldb::api::LogEntry;
namespace openmldb {
namespace log {
class WritableFile;
}
namespace storage {

static const ::openmldb::base::DefaultComparator scmp;

using ::openmldb::codec::SchemaCodec;

class SnapshotTest : public ::testing::Test {
 public:
    SnapshotTest() {}
    ~SnapshotTest() {}
};

inline uint32_t GenRand() { return rand() % 10000000 + 1; }

void RemoveData(const std::string& path) {
    ::openmldb::base::RemoveDir(path + "/data");
    ::openmldb::base::RemoveDir(path);
}

int GetManifest(const std::string file, ::openmldb::api::Manifest* manifest) {
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    google::protobuf::TextFormat::Parse(&fileInput, manifest);
    return 0;
}

bool RollWLogFile(WriteHandle** wh, LogParts* logs, const std::string& log_path, uint32_t& binlog_index,  // NOLINT
                  uint64_t offset, bool append_end = true) {
    if (*wh != NULL) {
        if (append_end) {
            (*wh)->EndLog();
        }
        delete *wh;
        *wh = NULL;
    }
    std::string name = ::openmldb::base::FormatToString(binlog_index, 8) + ".log";
    ::openmldb::base::MkdirRecur(log_path);
    std::string full_path = log_path + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    logs->Insert(binlog_index, offset);
    *wh = new WriteHandle("off", name, fd);
    binlog_index++;
    return true;
}

TEST_F(SnapshotTest, Recover_binlog_and_snapshot) {
    std::string snapshot_dir = FLAGS_db_root_path + "/4_3/snapshot/";
    std::string binlog_dir = FLAGS_db_root_path + "/4_3/binlog/";
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    WriteHandle* wh = nullptr;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    int count = 0;
    for (; count < 10; count++) {
        offset++;
        auto entry = ::openmldb::test::PackKVEntry(offset, "key", "value" + std::to_string(count), count, 0);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    MemTableSnapshot snapshot(4, 3, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("test", 4, 3, 8, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    uint64_t offset_value = 0;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    for (; count < 20; count++) {
        offset++;
        auto entry = ::openmldb::test::PackKVEntry(offset, "key2", "value" + std::to_string(count), count, 0);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    for (; count < 30; count++) {
        offset++;
        std::string key = "key" + std::to_string(count);
        auto entry = ::openmldb::test::PackKVEntry(offset, key, "value" + std::to_string(count), count, 0);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
        if (count == 25) {
            offset++;
            ::openmldb::api::LogEntry entry1;
            entry1.set_log_index(offset);
            entry1.set_method_type(::openmldb::api::MethodType::kDelete);
            ::openmldb::api::Dimension* dimension = entry1.add_dimensions();
            dimension->set_key(key);
            dimension->set_idx(0);
            entry1.set_term(5);
            std::string buffer1;
            entry1.SerializeToString(&buffer1);
            ::openmldb::base::Slice slice1(buffer1);
            ::openmldb::log::Status status = wh->Write(slice1);
        }
    }
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    // ASSERT_EQ(31u, latest_offset);
    Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewIterator("key", ticket));
    it->Seek(1);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1u, it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value1", ::openmldb::test::DecodeV(value2_str));
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(0u, it->GetKey());
    std::string value3_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value0", ::openmldb::test::DecodeV(value3_str));
    it->Next();
    ASSERT_FALSE(it->Valid());
    it.reset(table->NewIterator("key2", ticket));
    it->Seek(11);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(11u, it->GetKey());
    std::string value4_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value11", ::openmldb::test::DecodeV(value4_str));
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(10u, it->GetKey());
    std::string value5_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value10", ::openmldb::test::DecodeV(value5_str));
    it->Next();
    ASSERT_FALSE(it->Valid());
    it.reset(table->NewIterator("key23", ticket));
    it->Seek(23);
    ASSERT_TRUE(it->Valid());
    it.reset(table->NewIterator("key25", ticket));
    it->Seek(25);
    ASSERT_FALSE(it->Valid());
}

TEST_F(SnapshotTest, Recover_only_binlog_multi) {
    std::string snapshot_dir = FLAGS_db_root_path + "/4_4/snapshot/";
    std::string binlog_dir = FLAGS_db_root_path + "/4_4/binlog/";
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    auto meta = ::openmldb::test::GetTableMeta({"card", "merchant", "value"});
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    int count = 0;
    for (; count < 10; count++) {
        offset++;
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_ts(count + 1);
        std::string result;
        sdk_codec.EncodeRow({"card0", "merchant0", "value" + std::to_string(count)}, &result);
        entry.set_value(result);
        ::openmldb::api::Dimension* d1 = entry.add_dimensions();
        d1->set_key("card0");
        d1->set_idx(0);
        ::openmldb::api::Dimension* d2 = entry.add_dimensions();
        d2->set_key("merchant0");
        d2->set_idx(1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("card", 0));
    mapping.insert(std::make_pair("merchant", 1));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("test", 4, 4, 8, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    MemTableSnapshot snapshot(4, 4, log_part, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(10u, latest_offset);

    {
        Ticket ticket;
        std::unique_ptr<TableIterator> it(table->NewIterator(0, "card0", ticket));
        it->Seek(2);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(2u, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value2_str, &row);
        ASSERT_EQ("value1", row[2]);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(1u, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        row.clear();
        sdk_codec.DecodeRow(value3_str, &row);
        ASSERT_EQ("value0", row[2]);
        it->Next();
        ASSERT_FALSE(it->Valid());
    }

    {
        Ticket ticket;
        std::unique_ptr<TableIterator> it(table->NewIterator(1, "merchant0", ticket));
        it->Seek(2);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(2u, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value2_str, &row);
        ASSERT_EQ("value1", row[2]);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(1u, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        row.clear();
        sdk_codec.DecodeRow(value3_str, &row);
        ASSERT_EQ("value0", row[2]);
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
        auto entry = ::openmldb::test::PackKVEntry(offset, "key", "value" + std::to_string(count), count + 1, 0);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("test", 3, 3, 8, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    MemTableSnapshot snapshot(3, 3, log_part, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(10u, latest_offset);
    Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewIterator("key", ticket));
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2u, it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value1", ::openmldb::test::DecodeV(value2_str));
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1u, it->GetKey());
    std::string value3_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value0", ::openmldb::test::DecodeV(value3_str));
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_F(SnapshotTest, Recover_only_snapshot_multi) {
    std::string snapshot_dir = FLAGS_db_root_path + "/3_2/snapshot";
    std::string binlog_dir = FLAGS_db_root_path + "/3_2/binlog";

    ::openmldb::base::MkdirRecur(snapshot_dir);
    std::string snapshot1 = "20170609.sdb";
    auto meta = ::openmldb::test::GetTableMeta({"card", "merchant", "value"});
    ::openmldb::codec::SDKCodec sdk_codec(meta);
    {
        if (FLAGS_snapshot_compression != "off") {
            snapshot1.append(".");
            snapshot1.append(FLAGS_snapshot_compression);
        }
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::openmldb::log::WritableFile* wf = ::openmldb::log::NewWritableFile(snapshot1, fd_w);
        ::openmldb::log::Writer writer(FLAGS_snapshot_compression, wf);
        {
            ::openmldb::api::LogEntry entry;
            entry.set_ts(9527);
            std::string result;
            sdk_codec.EncodeRow({"card0", "merchant0", "test1"}, &result);
            entry.set_value(result);
            entry.set_log_index(1);
            ::openmldb::api::Dimension* d1 = entry.add_dimensions();
            d1->set_key("card0");
            d1->set_idx(0);
            ::openmldb::api::Dimension* d2 = entry.add_dimensions();
            d2->set_key("merchant0");
            d2->set_idx(1);
            std::string val;
            bool ok = entry.SerializeToString(&val);
            ASSERT_TRUE(ok);
            Slice sval(val.c_str(), val.size());
            ::openmldb::log::Status status = writer.AddRecord(sval);
            ASSERT_TRUE(status.ok());
        }
        {
            ::openmldb::api::LogEntry entry;
            entry.set_ts(9528);
            std::string result;
            sdk_codec.EncodeRow({"card0", "merchant0", "test2"}, &result);
            entry.set_value(result);
            entry.set_log_index(2);
            ::openmldb::api::Dimension* d1 = entry.add_dimensions();
            d1->set_key("card0");
            d1->set_idx(0);
            ::openmldb::api::Dimension* d2 = entry.add_dimensions();
            d2->set_key("merchant0");
            d2->set_idx(1);
            std::string val;
            bool ok = entry.SerializeToString(&val);
            ASSERT_TRUE(ok);
            Slice sval(val.c_str(), val.size());
            ::openmldb::log::Status status = writer.AddRecord(sval);
            ASSERT_TRUE(status.ok());
        }
        writer.EndLog();
    }

    {
        std::string snapshot2 = "20170610.sdb.tmp";
        std::string full_path = snapshot_dir + "/" + snapshot2;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::openmldb::log::WritableFile* wf = ::openmldb::log::NewWritableFile(snapshot2, fd_w);
        ::openmldb::log::Writer writer(FLAGS_snapshot_compression, wf);
        ::openmldb::api::LogEntry entry;
        entry.set_pk("test1");
        entry.set_ts(9527);
        entry.set_value("test1");
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        ::openmldb::log::Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry.set_pk("test1");
        entry.set_ts(9528);
        entry.set_value("test2");
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());
        writer.EndLog();
    }

    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("card", 0));
    mapping.insert(std::make_pair("merchant", 1));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("test", 3, 2, 8, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(3, 2, log_part, FLAGS_db_root_path);
    ASSERT_TRUE(snapshot.Init());
    int ret = snapshot.GenManifest(snapshot1, 3, 2, 5);
    ASSERT_EQ(0, ret);
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(2u, latest_offset);
    {
        Ticket ticket;
        std::unique_ptr<TableIterator> it(table->NewIterator(0, "card0", ticket));
        it->Seek(9528);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9528u, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value2_str, &row);
        ASSERT_EQ("test2", row[2]);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9527u, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        row.clear();
        sdk_codec.DecodeRow(value3_str, &row);
        ASSERT_EQ("test1", row[2]);
        it->Next();
        ASSERT_FALSE(it->Valid());
    }
    {
        Ticket ticket;
        std::unique_ptr<TableIterator> it(table->NewIterator(1, "merchant0", ticket));
        it->Seek(9528);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9528u, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value2_str, &row);
        ASSERT_EQ("test2", row[2]);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9527u, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        row.clear();
        sdk_codec.DecodeRow(value3_str, &row);
        ASSERT_EQ("test1", row[2]);
        it->Next();
        ASSERT_FALSE(it->Valid());
    }
    ASSERT_EQ(2u, table->GetRecordCnt());
    ASSERT_EQ(2u, table->GetRecordIdxCnt());
}

TEST_F(SnapshotTest, Recover_only_snapshot_multi_with_deleted_index) {
    std::string snapshot_dir = FLAGS_db_root_path + "/4_2/snapshot";
    std::string binlog_dir = FLAGS_db_root_path + "/4_2/binlog";

    ::openmldb::base::MkdirRecur(snapshot_dir);
    std::string snapshot1 = "20200309.sdb";
    ::openmldb::api::TableMeta* table_meta = new ::openmldb::api::TableMeta();
    table_meta->set_name("test");
    table_meta->set_tid(4);
    table_meta->set_pid(2);
    table_meta->set_seg_cnt(8);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "merchant", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "value", ::openmldb::type::kString);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "card", "card", "", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "merchant", "merchant", "", ::openmldb::type::kAbsoluteTime, 0,
                          0);
    ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
    {
        if (FLAGS_snapshot_compression != "off") {
            snapshot1.append(".");
            snapshot1.append(FLAGS_snapshot_compression);
        }
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::openmldb::log::WritableFile* wf = ::openmldb::log::NewWritableFile(snapshot1, fd_w);
        ::openmldb::log::Writer writer(FLAGS_snapshot_compression, wf);
        {
            ::openmldb::api::LogEntry entry;
            entry.set_ts(9527);
            std::string result;
            sdk_codec.EncodeRow({"card0", "merchant0", "test1"}, &result);
            entry.set_value(result);
            entry.set_log_index(1);
            ::openmldb::api::Dimension* d1 = entry.add_dimensions();
            d1->set_key("card0");
            d1->set_idx(0);
            ::openmldb::api::Dimension* d2 = entry.add_dimensions();
            d2->set_key("merchant0");
            d2->set_idx(1);
            std::string val;
            bool ok = entry.SerializeToString(&val);
            ASSERT_TRUE(ok);
            Slice sval(val.c_str(), val.size());
            ::openmldb::log::Status status = writer.AddRecord(sval);
            ASSERT_TRUE(status.ok());
        }
        {
            ::openmldb::api::LogEntry entry;
            entry.set_ts(9528);
            std::string result;
            sdk_codec.EncodeRow({"card0", "merchant0", "test2"}, &result);
            entry.set_value(result);
            entry.set_log_index(2);
            ::openmldb::api::Dimension* d1 = entry.add_dimensions();
            d1->set_key("card0");
            d1->set_idx(0);
            ::openmldb::api::Dimension* d2 = entry.add_dimensions();
            d2->set_key("merchant0");
            d2->set_idx(1);
            std::string val;
            bool ok = entry.SerializeToString(&val);
            ASSERT_TRUE(ok);
            Slice sval(val.c_str(), val.size());
            ::openmldb::log::Status status = writer.AddRecord(sval);
            ASSERT_TRUE(status.ok());
        }
        writer.EndLog();
    }

    {
        std::string snapshot2 = "20200310.sdb.tmp";
        std::string full_path = snapshot_dir + "/" + snapshot2;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::openmldb::log::WritableFile* wf = ::openmldb::log::NewWritableFile(snapshot2, fd_w);
        ::openmldb::log::Writer writer(FLAGS_snapshot_compression, wf);
        ::openmldb::api::LogEntry entry;
        entry.set_pk("test1");
        entry.set_ts(9527);
        entry.set_value("test1");
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        ::openmldb::log::Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry.set_pk("test1");
        entry.set_ts(9528);
        entry.set_value("test2");
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());
        writer.EndLog();
    }
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>(*table_meta);
    table->Init();
    table->DeleteIndex("merchant");
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(4, 2, log_part, FLAGS_db_root_path);
    ASSERT_TRUE(snapshot.Init());
    int ret = snapshot.GenManifest(snapshot1, 4, 2, 5);
    ASSERT_EQ(0, ret);
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(2u, latest_offset);
    {
        Ticket ticket;
        std::unique_ptr<TableIterator> it(table->NewIterator(0, "card0", ticket));
        it->Seek(9528);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9528u, it->GetKey());
        std::string value2_str(it->GetValue().data(), it->GetValue().size());
        std::vector<std::string> row;
        sdk_codec.DecodeRow(value2_str, &row);
        ASSERT_EQ("test2", row[2]);
        it->Next();
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(9527u, it->GetKey());
        std::string value3_str(it->GetValue().data(), it->GetValue().size());
        row.clear();
        sdk_codec.DecodeRow(value3_str, &row);
        ASSERT_EQ("test1", row[2]);
        it->Next();
        ASSERT_FALSE(it->Valid());
    }
    {
        Ticket ticket;
        TableIterator* it = table->NewIterator(1, "merchant0", ticket);
        ASSERT_TRUE(it == nullptr);
    }
    ASSERT_EQ(2u, table->GetRecordCnt());
    ASSERT_EQ(2u, table->GetRecordIdxCnt());
}

TEST_F(SnapshotTest, Recover_only_snapshot) {
    std::string snapshot_dir = FLAGS_db_root_path + "/2_2/snapshot";

    ::openmldb::base::MkdirRecur(snapshot_dir);
    std::string snapshot1 = "20170609.sdb";
    {
        if (FLAGS_snapshot_compression != "off") {
            snapshot1.append(".");
            snapshot1.append(FLAGS_snapshot_compression);
        }
        std::string full_path = snapshot_dir + "/" + snapshot1;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::openmldb::log::WritableFile* wf = ::openmldb::log::NewWritableFile(snapshot1, fd_w);
        ::openmldb::log::Writer writer(FLAGS_snapshot_compression, wf);
        auto entry = ::openmldb::test::PackKVEntry(1, "test0", "test1", 9527, 1);
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        ::openmldb::log::Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry = ::openmldb::test::PackKVEntry(2, "test0", "test2", 9528, 1);
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());
        writer.EndLog();
    }

    {
        std::string snapshot2 = "20170610.sdb.tmp";
        std::string full_path = snapshot_dir + "/" + snapshot2;
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::openmldb::log::WritableFile* wf = ::openmldb::log::NewWritableFile(snapshot2, fd_w);
        ::openmldb::log::Writer writer(FLAGS_snapshot_compression, wf);
        auto entry = ::openmldb::test::PackKVEntry(1, "test1", "test1", 9527, 1);
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        ::openmldb::log::Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        entry = ::openmldb::test::PackKVEntry(2, "test1", "test2", 9528, 1);
        ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval2(val.c_str(), val.size());
        status = writer.AddRecord(sval2);
        ASSERT_TRUE(status.ok());
        writer.EndLog();
    }

    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));

    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("test", 2, 2, 8, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(2, 2, log_part, FLAGS_db_root_path);
    ASSERT_TRUE(snapshot.Init());
    int ret = snapshot.GenManifest(snapshot1, 2, 2, 5);
    ASSERT_EQ(0, ret);
    uint64_t offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, offset));
    ASSERT_EQ(2u, offset);
    Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewIterator("test0", ticket));
    it->Seek(9528);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9528u, it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test2", ::openmldb::test::DecodeV(value2_str));
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9527u, it->GetKey());
    std::string value3_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("test1", ::openmldb::test::DecodeV(value3_str));
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_F(SnapshotTest, MakeSnapshot) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(1, 2, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("tx_log", 1, 1, 8, mapping, 2, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    std::string log_path = FLAGS_db_root_path + "/1_2/binlog/";
    std::string snapshot_path = FLAGS_db_root_path + "/1_2/snapshot/";
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset++);
    int count = 0;
    for (; count < 10; count++) {
        std::string key = "key" + std::to_string(count);
        auto entry = ::openmldb::test::PackKVEntry(offset, key,
                "value", ::baidu::common::timer::get_micros() / 1000, 5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
        if (count % 2 == 0) {
            ::openmldb::api::LogEntry entry1;
            entry1.set_log_index(offset);
            entry1.set_method_type(::openmldb::api::MethodType::kDelete);
            auto dimension = entry1.add_dimensions();
            dimension->set_key(key);
            dimension->set_idx(0);
            entry1.set_term(5);
            std::string buffer1;
            entry1.SerializeToString(&buffer1);
            ::openmldb::base::Slice slice1(buffer1);
            ::openmldb::log::Status status = wh->Write(slice1);
            offset++;
        }
        if (count % 4 == 0) {
            entry.set_log_index(offset);
            std::string buffer2;
            entry.SerializeToString(&buffer2);
            ::openmldb::base::Slice slice2(buffer2);
            ::openmldb::log::Status status = wh->Write(slice2);
            offset++;
        }
    }
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    for (; count < 30; count++) {
        auto entry = ::openmldb::test::PackKVEntry(offset, "key" + std::to_string(count), "value", 1, 6);
        if (count == 20) {
            // set one timeout key
            entry.set_ts(::baidu::common::timer::get_micros() / 1000 - 4 * 60 * 1000);
        } else {
            entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        }
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());

    std::string full_path = snapshot_path + "MANIFEST";
    ::openmldb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(38, (int64_t)manifest.offset());
    ASSERT_EQ(27, (int64_t)manifest.count());
    ASSERT_EQ(6, (int64_t)manifest.term());

    for (; count < 50; count++) {
        auto entry = ::openmldb::test::PackKVEntry(offset, "key" + std::to_string(count), "value",
                ::baidu::common::timer::get_micros() / 1000, 7);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_method_type(::openmldb::api::MethodType::kDelete);
        ::openmldb::api::Dimension* dimension = entry.add_dimensions();
        std::string key = "key9";
        dimension->set_key(key);
        dimension->set_idx(0);
        entry.set_term(5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }

    ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    vec.clear();
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }

    ASSERT_EQ(59, (int64_t)manifest.offset());
    ASSERT_EQ(46, (int64_t)manifest.count());
    ASSERT_EQ(7, (int64_t)manifest.term());
}

TEST_F(SnapshotTest, MakeSnapshot_with_delete_index) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(1, 3, log_part, FLAGS_db_root_path);
    snapshot.Init();
    ::openmldb::api::TableMeta* table_meta = new ::openmldb::api::TableMeta();
    table_meta->set_name("test");
    table_meta->set_tid(4);
    table_meta->set_pid(2);
    table_meta->set_seg_cnt(8);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "merchant", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "value", ::openmldb::type::kString);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "card", "card", "", ::openmldb::type::kAbsoluteTime, 2, 0);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "merchant", "merchant", "", ::openmldb::type::kAbsoluteTime, 2,
                          0);
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>(*table_meta);
    table->Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    std::string log_path = FLAGS_db_root_path + "/1_3/binlog/";
    std::string snapshot_path = FLAGS_db_root_path + "/1_3/snapshot/";
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset++);
    int count = 0;
    ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
    for (; count < 10; count++) {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_ts(::baidu::common::timer::get_micros() / 1000);

        auto result = entry.mutable_value();
        sdk_codec.EncodeRow({"card" + std::to_string(count), "merchant" + std::to_string(count), "value"}, result);
        entry.set_term(5);
        ::openmldb::api::Dimension* d1 = entry.add_dimensions();
        d1->set_key("card" + std::to_string(count));
        d1->set_idx(0);
        ::openmldb::api::Dimension* d2 = entry.add_dimensions();
        d2->set_key("merchant" + std::to_string(count));
        d2->set_idx(1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    for (; count < 30; count++) {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        ::openmldb::api::Dimension* d1 = entry.add_dimensions();
        d1->set_key("card" + std::to_string(count));
        d1->set_idx(0);
        ::openmldb::api::Dimension* d2 = entry.add_dimensions();
        d2->set_key("merchant" + std::to_string(count));
        d2->set_idx(1);
        entry.set_term(6);
        if (count == 20) {
            // set one timeout key
            entry.set_ts(::baidu::common::timer::get_micros() / 1000 - 4 * 60 * 1000);
        } else {
            entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        }
        auto result = entry.mutable_value();
        sdk_codec.EncodeRow({"card" + std::to_string(count), "merchant" + std::to_string(count), "value"}, result);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());

    std::string full_path = snapshot_path + "MANIFEST";
    ::openmldb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(30, (int64_t)manifest.offset());
    ASSERT_EQ(29, (int64_t)manifest.count());
    ASSERT_EQ(6, (int64_t)manifest.term());
    for (; count < 50; count++) {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        ::openmldb::api::Dimension* d1 = entry.add_dimensions();
        d1->set_key("card" + std::to_string(count));
        d1->set_idx(0);
        ::openmldb::api::Dimension* d2 = entry.add_dimensions();
        d2->set_key("merchant" + std::to_string(count));
        d2->set_idx(1);
        entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        auto result = entry.mutable_value();
        sdk_codec.EncodeRow({"card" + std::to_string(count), "merchant" + std::to_string(count), "value"}, result);
        entry.set_term(7);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_method_type(::openmldb::api::MethodType::kDelete);
        ::openmldb::api::Dimension* d1 = entry.add_dimensions();
        d1->set_key("card9");
        d1->set_idx(0);
        entry.set_term(5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }

    table->DeleteIndex("merchant");

    ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    vec.clear();
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }

    ASSERT_EQ(51, (int64_t)manifest.offset());
    ASSERT_EQ(48, (int64_t)manifest.count());
    ASSERT_EQ(7, (int64_t)manifest.term());
}

TEST_F(SnapshotTest, MakeSnapshotAbsOrLat) {
    ::openmldb::api::TableMeta* table_meta = new ::openmldb::api::TableMeta();
    table_meta->set_name("absorlat");
    table_meta->set_tid(10);
    table_meta->set_pid(0);
    table_meta->set_seg_cnt(8);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "merchant", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "ts", ::openmldb::type::kTimestamp);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "date", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta->add_column_desc(), "value", ::openmldb::type::kString);
    SchemaCodec::SetIndex(table_meta->add_column_key(), "index1", "card|merchant", "", ::openmldb::type::kAbsOrLat, 0,
                          1);
    std::shared_ptr<MemTable> table = std::make_shared<MemTable>(*table_meta);
    table->Init();

    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(10, 0, log_part, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    std::string log_path = FLAGS_db_root_path + "/10_0/binlog/";
    std::string snapshot_path = FLAGS_db_root_path + "/10_0/snapshot/";
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    ::openmldb::codec::SDKCodec sdk_codec(*table_meta);
    for (uint64_t i = 0; i < 3; i++) {
        offset++;
        ::openmldb::api::Dimension dimensions;
        dimensions.set_key("c0|m0");
        dimensions.set_idx(0);

        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        ::openmldb::api::Dimension* d_ptr = entry.add_dimensions();
        d_ptr->CopyFrom(dimensions);
        entry.set_ts(i + 1);
        auto result = entry.mutable_value();
        sdk_codec.EncodeRow({"c0", "m0", "1111122", "2021", "value"}, result);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);

        google::protobuf::RepeatedPtrField<::openmldb::api::Dimension> d_list;
        ::openmldb::api::Dimension* d_ptr2 = d_list.Add();
        d_ptr2->CopyFrom(dimensions);
        ASSERT_EQ(table->Put(i + 1, *result, d_list), true);
    }

    table->SchedGc();
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);

    std::string full_path = snapshot_path + "MANIFEST";
    ::openmldb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(1, (int64_t)manifest.count());
    ASSERT_EQ(3, (int64_t)manifest.offset());
}

TEST_F(SnapshotTest, MakeSnapshotLatest) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(5, 1, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("tx_log", 5, 1, 8, mapping, 4, ::openmldb::type::TTLType::kLatestTime);
    table->Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    std::string log_path = FLAGS_db_root_path + "/5_1/binlog/";
    std::string snapshot_path = FLAGS_db_root_path + "/5_1/snapshot/";
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    int count = 0;
    for (; count < 10; count++) {
        std::string key = "key" + std::to_string(count % 4);
        auto entry = ::openmldb::test::PackKVEntry(offset, key, "value", count + 1, 5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        table->Put(key, count, buffer.data(), buffer.size());
        offset++;
    }
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    for (; count < 30; count++) {
        std::string key = "key" + std::to_string(count % 4);
        auto entry = ::openmldb::test::PackKVEntry(offset, key, "value", count + 1, 6);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        table->Put(key, count, buffer.data(), buffer.size());
        offset++;
    }
    table->SchedGc();
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());

    std::string full_path = snapshot_path + "MANIFEST";
    ::openmldb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(offset - 1, manifest.offset());
    ASSERT_EQ(16, (int64_t)manifest.count());
    ASSERT_EQ(6, (int64_t)manifest.term());

    for (; count < 1000; count++) {
        std::string key = "key1000";
        if (count == 100) {
            key = "key2222";
        }
        auto entry = ::openmldb::test::PackKVEntry(offset, key, "value", count, 7);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        table->Put(key, count, buffer.data(), buffer.size());
        offset++;
    }
    table->SchedGc();
    ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    vec.clear();
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }

    ASSERT_EQ(offset - 1, manifest.offset());
    ASSERT_EQ(21, (int64_t)manifest.count());
    ASSERT_EQ(7, (int64_t)manifest.term());
}

TEST_F(SnapshotTest, RecordOffset) {
    std::string snapshot_path = FLAGS_db_root_path + "/1_1/snapshot/";
    MemTableSnapshot snapshot(1, 1, NULL, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t offset = 1122;
    uint64_t key_count = 3000;
    uint64_t term = 0;
    std::string snapshot_name = ::openmldb::base::GetNowTime() + ".sdb";
    int ret = snapshot.GenManifest(snapshot_name, key_count, offset, term);
    ASSERT_EQ(0, ret);
    std::string value;
    ::openmldb::api::Manifest manifest;
    GetManifest(snapshot_path + "MANIFEST", &manifest);
    ASSERT_EQ(offset, manifest.offset());
    ASSERT_EQ(term, manifest.term());
    sleep(1);

    std::string snapshot_name1 = ::openmldb::base::GetNowTime() + ".sdb";
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
        auto entry = ::openmldb::test::PackKVEntry(offset, "key", "value" + std::to_string(count), count + 1, 1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
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
        auto entry = ::openmldb::test::PackKVEntry(offset, "key_new",
                "value_new" + std::to_string(count), count + 1, 1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    binlog_index++;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset, false);
    count = 0;
    for (; count < 10; count++) {
        offset++;
        auto entry = ::openmldb::test::PackKVEntry(offset, "key_xxx",
                "value_xxx" + std::to_string(count), count + 1, 1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    delete wh;

    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("test", tid, 0, 8, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    MemTableSnapshot snapshot(tid, 0, log_part, FLAGS_db_root_path);
    snapshot.Init();
    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, offset));
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(30u, latest_offset);
    Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewIterator("key_new", ticket));
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2, (int64_t)it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value_new1", ::openmldb::test::DecodeV(value2_str));
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, (int64_t)it->GetKey());
    std::string value3_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value_new0", ::openmldb::test::DecodeV(value3_str));
    it->Next();
    ASSERT_FALSE(it->Valid());
    it.reset(table->NewIterator("key_xxx", ticket));
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2, (int64_t)it->GetKey());
    std::string value4_str(it->GetValue().data(), it->GetValue().size());
    ASSERT_EQ("value_xxx1", ::openmldb::test::DecodeV(value4_str));

    // check snapshot
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::openmldb::base::GetFileName(snapshot_dir, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    std::string full_path = snapshot_dir + "MANIFEST";
    ::openmldb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(30, (int64_t)manifest.offset());
    ASSERT_EQ(30, (int64_t)manifest.count());
}

TEST_F(SnapshotTest, Recover_snapshot_ts) {
    std::string snapshot_dir = FLAGS_db_root_path + "/2_2/snapshot";
    ::openmldb::api::TableMeta table_meta;
    table_meta.set_name("test");
    table_meta.set_tid(2);
    table_meta.set_pid(2);
    table_meta.set_seg_cnt(8);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "card", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "mcc", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "amt", ::openmldb::type::kDouble);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts1", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "ts2", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(table_meta.add_column_desc(), "value", ::openmldb::type::kString);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card", "card", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "card1", "card", "ts2", ::openmldb::type::kAbsoluteTime, 0, 0);
    SchemaCodec::SetIndex(table_meta.add_column_key(), "mcc", "mcc", "ts1", ::openmldb::type::kAbsoluteTime, 0, 0);
    table_meta.set_mode(::openmldb::api::TableMode::kTableLeader);
    ::openmldb::base::MkdirRecur(snapshot_dir);
    std::string snapshot1 = "20190614.sdb";
    ::openmldb::codec::SDKCodec sdk_codec(table_meta);
    {
        if (FLAGS_snapshot_compression != "off") {
            snapshot1.append(".");
            snapshot1.append(FLAGS_snapshot_compression);
        }
        std::string full_path = snapshot_dir + "/" + snapshot1;
        printf("path:%s\n", full_path.c_str());
        FILE* fd_w = fopen(full_path.c_str(), "ab+");
        ASSERT_TRUE(fd_w != NULL);
        ::openmldb::log::WritableFile* wf = ::openmldb::log::NewWritableFile(snapshot1, fd_w);
        ::openmldb::log::Writer writer(FLAGS_snapshot_compression, wf);
        ::openmldb::api::LogEntry entry;
        entry.set_ts(9527);
        auto result = entry.mutable_value();
        sdk_codec.EncodeRow({"card0", "mcc0", "1.3", "1122", "2233", "value0"}, result);
        entry.set_log_index(1);
        ::openmldb::api::Dimension* dim = entry.add_dimensions();
        dim->set_key("card0");
        dim->set_idx(0);
        dim = entry.add_dimensions();
        dim->set_key("card0");
        dim->set_idx(1);
        dim = entry.add_dimensions();
        dim->set_key("mcc0");
        dim->set_idx(2);
        ::openmldb::api::TSDimension* ts_dim = entry.add_ts_dimensions();
        ts_dim->set_ts(1122);
        ts_dim->set_idx(0);
        ::openmldb::api::TSDimension* ts_dim1 = entry.add_ts_dimensions();
        ts_dim1->set_ts(2233);
        ts_dim1->set_idx(1);
        std::string val;
        bool ok = entry.SerializeToString(&val);
        ASSERT_TRUE(ok);
        Slice sval(val.c_str(), val.size());
        ::openmldb::log::Status status = writer.AddRecord(sval);
        ASSERT_TRUE(status.ok());
        writer.EndLog();
    }

    std::shared_ptr<MemTable> table = std::make_shared<MemTable>(table_meta);
    table->Init();
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(2, 2, log_part, FLAGS_db_root_path);
    ASSERT_TRUE(snapshot.Init());
    int ret = snapshot.GenManifest(snapshot1, 1, 1, 5);
    ASSERT_EQ(0, ret);
    uint64_t offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, offset));
    ASSERT_EQ(1u, offset);
    Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewIterator(0, "card0", ticket));
    it->Seek(1122);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1122, (int64_t)it->GetKey());
    std::string value2_str(it->GetValue().data(), it->GetValue().size());
    std::vector<std::string> row;
    sdk_codec.DecodeRow(value2_str, &row);
    ASSERT_EQ("value0", row[5]);
    it->Next();
    ASSERT_FALSE(it->Valid());
    it.reset(table->NewIterator(1, "card0", ticket));
    it->Seek(2233);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2233, (int64_t)it->GetKey());
    value2_str.assign(it->GetValue().data(), it->GetValue().size());
    row.clear();
    sdk_codec.DecodeRow(value2_str, &row);
    ASSERT_EQ("value0", row[5]);
    it->Next();
    ASSERT_FALSE(it->Valid());
    it.reset(table->NewIterator(2, "mcc0", ticket));
    it->Seek(1122);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1122, (int64_t)it->GetKey());
    value2_str.assign(it->GetValue().data(), it->GetValue().size());
    row.clear();
    sdk_codec.DecodeRow(value2_str, &row);
    ASSERT_EQ("value0", row[5]);
    it->Next();
    ASSERT_FALSE(it->Valid());
    it.reset(table->NewIterator(0, "mcc0", ticket));
    it->Seek(1122);
    ASSERT_FALSE(it->Valid());
}

TEST_F(SnapshotTest, MakeSnapshotWithEndOffset) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    MemTableSnapshot snapshot(10, 2, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("tx_log", 1, 10, 8, mapping, 2, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    std::string log_path = FLAGS_db_root_path + "/10_2/binlog/";
    std::string snapshot_path = FLAGS_db_root_path + "/10_2/snapshot/";
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset++);
    int count = 0;
    for (; count < 10; count++) {
        std::string key = "key" + std::to_string(count);
        auto entry = ::openmldb::test::PackKVEntry(offset, key, "value",
                ::baidu::common::timer::get_micros() / 1000, 5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
        if (count % 2 == 0) {
            ::openmldb::api::LogEntry entry1;
            entry1.set_log_index(offset);
            entry1.set_method_type(::openmldb::api::MethodType::kDelete);
            ::openmldb::api::Dimension* dimension = entry1.add_dimensions();
            dimension->set_key(key);
            dimension->set_idx(0);
            entry1.set_term(5);
            std::string buffer1;
            entry1.SerializeToString(&buffer1);
            ::openmldb::base::Slice slice1(buffer1);
            ::openmldb::log::Status status = wh->Write(slice1);
            offset++;
        }
        if (count % 4 == 0) {
            entry.set_log_index(offset);
            std::string buffer2;
            entry.SerializeToString(&buffer2);
            ::openmldb::base::Slice slice2(buffer2);
            ::openmldb::log::Status status = wh->Write(slice2);
            offset++;
        }
    }
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    for (; count < 30; count++) {
        std::string key = "key" + std::to_string(count);
        auto entry = ::openmldb::test::PackKVEntry(offset, key, "value", 1, 6);
        if (count == 20) {
            // set one timeout key
            entry.set_ts(::baidu::common::timer::get_micros() / 1000 - 4 * 60 * 1000);
        } else {
            entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        }
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value, 18);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());

    std::string full_path = snapshot_path + "MANIFEST";
    ::openmldb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(18, (int64_t)manifest.offset());
    ASSERT_EQ(8, (int64_t)manifest.count());
    ASSERT_EQ(5, (int64_t)manifest.term());

    ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(4, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());

    full_path = snapshot_path + "MANIFEST";
    manifest.Clear();
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(38, (int64_t)manifest.offset());
    ASSERT_EQ(27, (int64_t)manifest.count());
    ASSERT_EQ(6, (int64_t)manifest.term());

    for (; count < 50; count++) {
        std::string key = "key" + std::to_string(count);
        auto entry = ::openmldb::test::PackKVEntry(offset, key, "value",
                ::baidu::common::timer::get_micros() / 1000, 7);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }
    {
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_method_type(::openmldb::api::MethodType::kDelete);
        ::openmldb::api::Dimension* dimension = entry.add_dimensions();
        std::string key = "key9";
        dimension->set_key(key);
        dimension->set_idx(0);
        entry.set_term(5);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        offset++;
    }
    // end_offset less than last make snapshot offset, MakeSnapshot will fail.
    ret = snapshot.MakeSnapshot(table, offset_value, 5);
    ASSERT_EQ(-1, ret);
    ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    vec.clear();
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(2, (int32_t)vec.size());
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }

    ASSERT_EQ(59, (int64_t)manifest.offset());
    ASSERT_EQ(46, (int64_t)manifest.count());
    ASSERT_EQ(7, (int64_t)manifest.term());
}

TEST_F(SnapshotTest, Recover_large_snapshot) {
    std::string snapshot_dir = FLAGS_db_root_path + "/100_0/snapshot/";
    std::string binlog_dir = FLAGS_db_root_path + "/100_0/binlog/";
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    int count = 0;
    uint64_t start_time = ::baidu::common::timer::get_micros();
    for (; count < 1000000; count++) {
        offset++;
        auto entry = ::openmldb::test::PackKVEntry(offset, "key", "value" + std::to_string(count), count + 1, 1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    MemTableSnapshot snapshot(100, 0, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("test", 100, 0, 8, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    uint64_t offset_value = 0;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);

    uint64_t snapshot_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));

    uint64_t end_time = ::baidu::common::timer::get_micros();
    std::cout << "use time in us: " << end_time - start_time << std::endl;

    ASSERT_EQ(1000000u, snapshot_offset);
    Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewIterator("key", ticket));
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    uint64_t num = 1000000;
    while (it->Valid()) {
        num--;
        ASSERT_EQ(num + 1, it->GetKey());
        std::string value_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ("value" + std::to_string(num), ::openmldb::test::DecodeV(value_str));
        it->Next();
    }
    ASSERT_EQ(0u, num);
    RemoveData(FLAGS_db_root_path);
}

TEST_F(SnapshotTest, Recover_large_snapshot_and_binlog) {
    std::string snapshot_dir = FLAGS_db_root_path + "/101_0/snapshot/";
    std::string binlog_dir = FLAGS_db_root_path + "/101_0/binlog/";
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    uint32_t count = 0;
    std::string base_str = std::string(50 * 1024 * 1024, 'a');
    // if (FLAGS_snapshot_compression != "off") {
    //     base_str = std::string(4 * 1024 * 1024, 'a');
    // }
    uint32_t total_num = 10;
    uint64_t start_time = ::baidu::common::timer::get_micros();
    for (; count < total_num; count++) {
        offset++;
        auto entry = ::openmldb::test::PackKVEntry(offset, "key", base_str + std::to_string(count), count + 1, 1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();
    std::cout << "sync end" << std::endl;
    MemTableSnapshot snapshot(101, 0, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    std::shared_ptr<MemTable> table =
        std::make_shared<MemTable>("test", 100, 0, 8, mapping, 0, ::openmldb::type::TTLType::kAbsoluteTime);
    table->Init();
    uint64_t offset_value = 0;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);

    RollWLogFile(&wh, log_part, binlog_dir, binlog_index, offset);
    for (; count < total_num * 2; count++) {
        offset++;
        auto entry = ::openmldb::test::PackKVEntry(offset, "key", base_str + std::to_string(count), count + 1, 1);
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::openmldb::base::Slice slice(buffer);
        ::openmldb::log::Status status = wh->Write(slice);
        ASSERT_TRUE(status.ok());
    }
    wh->Sync();

    uint64_t snapshot_offset = 0;
    uint64_t latest_offset = 0;
    ASSERT_TRUE(snapshot.Recover(table, snapshot_offset));
    ASSERT_EQ(total_num, snapshot_offset);
    Binlog binlog(log_part, binlog_dir);
    binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset);
    ASSERT_EQ(total_num * 2, latest_offset);

    uint64_t end_time = ::baidu::common::timer::get_micros();
    std::cout << "use time in us: " << end_time - start_time << std::endl;

    Ticket ticket;
    std::unique_ptr<TableIterator> it(table->NewIterator("key", ticket));
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    uint64_t num = total_num * 2;
    while (it->Valid()) {
        num--;
        ASSERT_EQ(num + 1, it->GetKey());
        std::string value_str(it->GetValue().data(), it->GetValue().size());
        ASSERT_EQ(base_str + std::to_string(num), ::openmldb::test::DecodeV(value_str));
        it->Next();
    }
    ASSERT_EQ(0u, num);
    RemoveData(FLAGS_db_root_path);
}

TEST_F(SnapshotTest, DeleteRange) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    uint32_t tid = GenRand();
    uint32_t pid = 2;
    MemTableSnapshot snapshot(tid, pid, log_part, FLAGS_db_root_path);
    snapshot.Init();
    std::map<std::string, uint32_t> mapping = { {"idx0", 0} };
    auto table = std::make_shared<MemTable>("tx_log", tid, pid, 8, mapping, 0, ::openmldb::type::TTLType::kLatestTime);
    table->Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
    std::string log_path = absl::StrCat(FLAGS_db_root_path, "/", tid, "_", pid, "/binlog/");
    std::string snapshot_path = absl::StrCat(FLAGS_db_root_path, "/", tid, "_", pid, "/snapshot/");
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset++);
    uint64_t ts = 1000;
    std::string buffer;
    for (int count = 0; count < 10; count++) {
        std::string key = "key" + std::to_string(count);
        for (int i = 0; i < 10; i++) {
            auto entry = ::openmldb::test::PackKVEntry(offset, key, "value", ts + i, 5);
            entry.SerializeToString(&buffer);
            ::openmldb::log::Status status = wh->Write(base::Slice(buffer));
            offset++;
        }
        ::openmldb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_method_type(::openmldb::api::MethodType::kDelete);
        auto dimension = entry.add_dimensions();
        dimension->set_key(key);
        dimension->set_idx(0);
        entry.set_term(5);
        if (count % 2 == 0) {
            entry.set_ts(1005);
            entry.set_end_ts(1003);
        } else {
            entry.set_ts(1005);
        }
        entry.SerializeToString(&buffer);
        ::openmldb::log::Status status = wh->Write(base::Slice(buffer));
        offset++;
    }
    ::openmldb::api::LogEntry entry;
    entry.set_log_index(offset);
    entry.set_method_type(::openmldb::api::MethodType::kDelete);
    entry.set_ts(1008);
    entry.set_end_ts(1006);
    entry.SerializeToString(&buffer);
    ::openmldb::log::Status status = wh->Write(base::Slice(buffer));
    uint64_t offset_value;
    int ret = snapshot.MakeSnapshot(table, offset_value, 0);
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::openmldb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, (int32_t)vec.size());
    vec.clear();
    ret = ::openmldb::base::GetFileName(log_path, vec);
    ASSERT_EQ(1, (int32_t)vec.size());

    std::string full_path = snapshot_path + "MANIFEST";
    ::openmldb::api::Manifest manifest;
    {
        int fd = open(full_path.c_str(), O_RDONLY);
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        google::protobuf::TextFormat::Parse(&fileInput, &manifest);
    }
    ASSERT_EQ(111, (int64_t)manifest.offset());
    ASSERT_EQ(40, (int64_t)manifest.count());
    ASSERT_EQ(5, (int64_t)manifest.term());
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::base::SetLogLevel(DEBUG);
    int ret = 0;
    std::vector<std::string> vec{"off", "zlib", "snappy"};
    ::openmldb::test::TempPath tmp_path;
    for (size_t i = 0; i < vec.size(); i++) {
        std::cout << "compress type: " << vec[i] << std::endl;
        FLAGS_db_root_path = tmp_path.GetTempPath();
        FLAGS_snapshot_compression = vec[i];
        ret += RUN_ALL_TESTS();
    }
    return ret;
}
