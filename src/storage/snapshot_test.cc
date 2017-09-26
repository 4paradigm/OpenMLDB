//
// segment_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "gtest/gtest.h"
#include "logging.h"
#include "storage/snapshot.h"
#include "storage/table.h"
#include "storage/ticket.h"
#include "proto/tablet.pb.h"
#include "gflags/gflags.h"
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <time.h>
#include "base/strings.h"
#include "base/file_util.h"
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

DECLARE_string(db_root_path);

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace storage {

const static ::rtidb::base::DefaultComparator scmp;

class SnapshotTest : public ::testing::Test {

public:
    SnapshotTest(){}
    ~SnapshotTest() {}
};

inline std::string GenRand() {
    return boost::lexical_cast<std::string>(rand() % 10000000 + 1);
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
			uint32_t& binlog_index, uint64_t offset) {
    if (*wh != NULL) {
        (*wh)->EndLog();
        delete *wh;
        *wh = NULL;
    }
    std::string name = ::rtidb::base::FormatToString(binlog_index, 10) + ".log";
    std::string full_path = log_path + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        LOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    logs->Insert(binlog_index, offset);
    *wh = new WriteHandle(name, fd);
    binlog_index++;
    return true;
}

TEST_F(SnapshotTest, Recover) {
}

TEST_F(SnapshotTest, MakeSnapshot) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    Snapshot snapshot(1, 2, log_part);
    snapshot.Init();
    Table* table = new Table("tx_log", 1, 1, 8 , 1440000);
    table->Ref();
    table->Init();
    uint64_t offset = 0;
    uint32_t binlog_index = 0;
	std::string log_path = FLAGS_db_root_path + "/1_2/binlog/";
	std::string snapshot_path = FLAGS_db_root_path + "/1_2/snapshot/";
    WriteHandle* wh = NULL;
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    int count = 0;
    for (; count < 10; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + boost::lexical_cast<std::string>(count);
        entry.set_pk(key);
        entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        entry.set_value("value");
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        offset++;
    }
    RollWLogFile(&wh, log_part, log_path, binlog_index, offset);
    for (; count < 30; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + boost::lexical_cast<std::string>(count);
        entry.set_pk(key);
        entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        entry.set_value("value");
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        offset++;
    }
    
    int ret = snapshot.MakeSnapshot(table);
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
    ASSERT_EQ(29, manifest.offset());
    ASSERT_EQ(29, manifest.count());

    for (; count < 50; count++) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        std::string key = "key" + boost::lexical_cast<std::string>(count);
        entry.set_pk(key);
        entry.set_ts(::baidu::common::timer::get_micros() / 1000);
        entry.set_value("value");
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh->Write(slice);
        offset++;
    }
    ret = snapshot.MakeSnapshot(table);
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

    ASSERT_EQ(49, manifest.offset());
    ASSERT_EQ(49, manifest.count());

    table->UnRef();

}

TEST_F(SnapshotTest, RecordOffset) {
	std::string snapshot_path = FLAGS_db_root_path + "/1_1/snapshot/";
    Snapshot snapshot(1, 1, NULL);
    snapshot.Init();
    uint64_t offset = 1122;
    uint64_t key_count = 3000;
    std::string snapshot_name = ::rtidb::base::GetNowTime() + ".sdb";
    int ret = snapshot.RecordOffset(snapshot_name, key_count, offset);
    ASSERT_EQ(0, ret);
	std::string value;
	::rtidb::api::Manifest manifest;
	GetManifest(snapshot_path + "MANIFEST", &manifest);
    ASSERT_EQ(offset, manifest.offset());
    sleep(1);

    std::string snapshot_name1 = ::rtidb::base::GetNowTime() + ".sdb";
    uint64_t key_count1 = 3001;
	offset = 1124;
    ret = snapshot.RecordOffset(snapshot_name1, key_count1, offset);
    ASSERT_EQ(0, ret);
	GetManifest(snapshot_path + "MANIFEST", &manifest);
    ASSERT_EQ(offset, manifest.offset());
    ASSERT_EQ(key_count1, manifest.count());
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    FLAGS_db_root_path = "/tmp/" + ::rtidb::storage::GenRand();
    return RUN_ALL_TESTS();
}


