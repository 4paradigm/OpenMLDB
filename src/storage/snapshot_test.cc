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

int GetFileContent(const std::string file, std::string& value) {
    FILE* fd_read = fopen(file.c_str(), "r");
    if (fd_read == NULL) {
        return -1;
    }
	value.clear();
	char buffer[1024];
	while (!feof(fd_read)) {
		if (fgets(buffer, 1024, fd_read) == NULL && !feof(fd_read)) {
			fclose(fd_read);
			return -1;
		}
		value.append(buffer);
	}
	fclose(fd_read);
    return 0;
}

TEST_F(SnapshotTest, Recover) {
}

TEST_F(SnapshotTest, MakeSnapshot) {
    LogParts* log_part = new LogParts(12, 4, scmp);
    Snapshot snapshot(1, 2, log_part);
    snapshot.Init();
    uint64_t offset = 0;
    int binlog_index = 0;
    log_part->Insert(binlog_index, offset);
	std::string log_path = FLAGS_db_root_path + "/1_2/binlog/";
	std::string snapshot_path = FLAGS_db_root_path + "/1_2/snapshot/";
    std::string name = ::rtidb::base::FormatToString(0, 10) + ".log";
    std::string full_path = log_path + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        printf("open file[%s] failed!\n", full_path.c_str());
        return;
    }
    WriteHandle* wh_ = new WriteHandle(name, fd);
    int count = 10;
    while (count--) {
        ::rtidb::api::LogEntry entry;
        entry.set_log_index(offset);
        entry.set_pk("11234");
        entry.set_ts(count);
        entry.set_value("value");
        std::string buffer;
        entry.SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer);
        ::rtidb::base::Status status = wh_->Write(slice);
        offset++;
    }
    
    int ret = snapshot.MakeSnapshot();
    ASSERT_EQ(0, ret);
    std::vector<std::string> vec;
    ret = ::rtidb::base::GetFileName(snapshot_path, vec);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, vec.size());
}

TEST_F(SnapshotTest, RecordOffset) {
	std::string snapshot_path = FLAGS_db_root_path + "/1_1/snapshot/";
    Snapshot snapshot(1, 1, NULL);
    snapshot.Init();
    uint64_t offset = 1122;
    uint64_t key_count = 3000;
    ::rtidb::api::LogEntry entry;
    entry.set_log_index(offset);
    std::string buffer;
    entry.SerializeToString(&buffer);
    std::string snapshot_name = ::rtidb::base::GetNowTime() + ".sdb";
    int ret = snapshot.RecordOffset(buffer, snapshot_name, key_count);
    ASSERT_EQ(0, ret);
	std::string value;
	GetFileContent(snapshot_path + "MANIFEST", value);
	::rtidb::api::Manifest manifest;
	manifest.ParseFromString(value);
    ASSERT_EQ(offset, manifest.offset());
    sleep(1);

    std::string snapshot_name1 = ::rtidb::base::GetNowTime() + ".sdb";
    uint64_t key_count1 = 3001;
	offset = 1124;
    entry.set_log_index(offset);
	buffer.clear();
    entry.SerializeToString(&buffer);
    ret = snapshot.RecordOffset(buffer, snapshot_name1, key_count1);
    ASSERT_EQ(0, ret);
	GetFileContent(snapshot_path + "MANIFEST", value);
	manifest.ParseFromString(value);
    ASSERT_EQ(offset, manifest.offset());
    ASSERT_EQ(2, manifest.snapshot_infos_size());
	if (manifest.snapshot_infos(0).name() == snapshot_name) {
    	ASSERT_EQ(key_count, manifest.snapshot_infos(0).count());
    	ASSERT_EQ(key_count1, manifest.snapshot_infos(1).count());
	} else {
    	ASSERT_EQ(key_count, manifest.snapshot_infos(1).count());
    	ASSERT_EQ(key_count1, manifest.snapshot_infos(0).count());
	}	
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


