//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author yangjun
// Date 2018-01-07
//

#include "storage/disktable.h"
#include "gtest/gtest.h"
#include "timer.h"
#include "logging.h"
#include "base/file_util.h"

using ::baidu::common::INFO;

DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);

namespace rtidb {
namespace storage {

class DiskTableTest : public ::testing::Test {

public:
    DiskTableTest() {}
    ~DiskTableTest() {}
};

TEST_F(DiskTableTest, Put) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("yjtable1", 1, 1, mapping, 10, ::rtidb::api::TTLType::kAbsoluteTime);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    std::string raw_key = "test35";
    DiskTableIterator* it = table->NewIterator(raw_key);
    it->SeekToFirst();
    for (int k = 0; k < 10; k++) {
        ASSERT_TRUE(it->Valid());
        std::string pk = it->GetPK();
        ASSERT_EQ(pk, raw_key);
        ASSERT_EQ(9537 + 9 - k, it->GetKey());
        std::string value1 = it->GetValue().ToString();
        ASSERT_EQ("value", value1);
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    delete it;
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_1";
    ASSERT_TRUE(::rtidb::base::RemoveDir(path));
}

TEST_F(DiskTableTest, MultiDimensionPut) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable("yjtable2", 1, 2, mapping, 10, ::rtidb::api::TTLType::kAbsoluteTime);
    ASSERT_TRUE(table->Init());
    ASSERT_EQ(3, table->GetIdxCnt());
//    ASSERT_EQ(0, table->GetRecordIdxCnt());
//    ASSERT_EQ(0, table->GetRecordCnt());
    Dimensions  dimensions;
    ::rtidb::api::Dimension* d0 = dimensions.Add();
    d0->set_key("yjdim0");
    d0->set_idx(0);

    ::rtidb::api::Dimension* d1 = dimensions.Add();
    d1->set_key("yjdim1");
    d1->set_idx(1);

    ::rtidb::api::Dimension* d2 = dimensions.Add();
    d2->set_key("yjdim2");
    d2->set_idx(2);
    bool ok = table->Put(1, "yjtestvalue", dimensions);
    ASSERT_TRUE(ok);
//    ASSERT_EQ(3, table->GetRecordIdxCnt());
    DiskTableIterator* it = table->NewIterator(0, "yjdim0");
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    uint64_t ts = it->GetKey();
    ASSERT_EQ(1, ts);
    std::string value1 = it->GetValue().ToString();
    ASSERT_EQ("yjtestvalue", value1);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

    it = table->NewIterator(1, "yjdim1");
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    value1 = it->GetValue().ToString();
    ASSERT_EQ("yjtestvalue", value1);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

    it = table->NewIterator(2, "yjdim2");
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(1, ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("yjtestvalue", value1);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

    dimensions.Clear();
    d0 = dimensions.Add();
    d0->set_key("key2");
    d0->set_idx(0);

    d1 = dimensions.Add();
    d1->set_key("key1");
    d1->set_idx(1);

    d2 = dimensions.Add();
    d2->set_key("dimxxx1");
    d2->set_idx(2);
    ASSERT_TRUE(table->Put(2, "value2", dimensions));

    it = table->NewIterator(0, "key2");
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1");
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    delete it;

    std::string val;
    ASSERT_TRUE(table->Get(1, "key1", 2, val));
    ASSERT_EQ("value2", val);


    it = table->NewIterator(2, "dimxxx1");
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1");
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1");
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2, it->GetKey());
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_2";
    ASSERT_TRUE(::rtidb::base::RemoveDir(path));
}

TEST_F(DiskTableTest, Delete) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable("yjtable2", 1, 2, mapping, 10, ::rtidb::api::TTLType::kAbsoluteTime);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 10; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    DiskTableIterator* it = table->NewIterator("test6");
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();
        ASSERT_EQ("test6", pk);
        count++;
        it->Next();
    }
    ASSERT_EQ(count, 10);
    delete it;
    table->Delete("test6", 0);
    it = table->NewIterator("test6");
    it->SeekToFirst();
    ASSERT_FALSE(it->Valid());
    delete it;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_2";
    ASSERT_TRUE(::rtidb::base::RemoveDir(path));
}

TEST_F(DiskTableTest, TraverseIterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 3, mapping, 0, ::rtidb::api::TTLType::kAbsoluteTime);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
            if (idx == 10 && k == 5) {
                ASSERT_TRUE(table->Put(key, ts + k, "valu9", 5));
                ASSERT_TRUE(table->Put(key, ts + k, "valu8", 5));
            }
        }
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();;
        count++;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_EQ(1000, count);

    it->Seek("test90", 9543);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();;
            ASSERT_EQ("test90", pk);
            ASSERT_EQ(9542, it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(96, count);

    it->Seek("test90", 9537);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();;
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9546, it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(90, count);

    it->Seek("test90", 9530);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();;
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9546, it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(90, count);

    ASSERT_TRUE(table->Put("test98", 9548, "valu8", 5));
    it->Seek("test98", 9547);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();;
            ASSERT_EQ("test98", pk);
            ASSERT_EQ(9546, it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(20, count);
    std::string val;
    ASSERT_TRUE(table->Get(0, "test98", 9548, val));
    ASSERT_EQ("valu8", val);
    delete it;
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_3";
    ASSERT_TRUE(::rtidb::base::RemoveDir(path));
}

TEST_F(DiskTableTest, TraverseIteratorLatest) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 3, mapping, 3, ::rtidb::api::TTLType::kLatestTime);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 5; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
            if (idx == 10 && k == 2) {
                ASSERT_TRUE(table->Put(key, ts + k, "valu9", 5));
                ASSERT_TRUE(table->Put(key, ts + k, "valu8", 5));
            }
        }
    }
    TableIterator* it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();;
        count++;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_EQ(300, count);

    it->Seek("test90", 9541);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();;
            ASSERT_EQ("test90", pk);
            ASSERT_EQ(9540, it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(29, count);

    it->Seek("test90", 9537);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();;
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9541, it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(27, count);
    it->Seek("test90", 9530);
    count = 0;
    while (it->Valid()) {
        if (count == 0) {
            std::string pk = it->GetPK();;
            ASSERT_EQ("test91", pk);
            ASSERT_EQ(9541, it->GetKey());
        }
        count++;
        it->Next();
    }
    ASSERT_EQ(27, count);
    delete it;
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_3";
    ASSERT_TRUE(::rtidb::base::RemoveDir(path));
}

/* No need to test for ondisktable
TEST_F(DiskTableTest, MultiDimissionPut1) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0|6", 0));
    mapping.insert(std::make_pair("idx1|6", 1));
    mapping.insert(std::make_pair("idx2|6", 2));
    DiskTable* table = new DiskTable("yjtable3", 1, 1, 8, mapping, 10);
    table->Init();
    table->CreateWithPath("/tmp/yjtest/yjtable3");
    PDLOG(INFO, "CreateWithPath yjtable3");
    ASSERT_EQ(3, table->GetIdxCnt());
    DataBlock* db = new DataBlock(3, "helloworld", 10);
    std::string d1 = "d1";
    ASSERT_FALSE(table->Put(d1, 9527, db, 3));
    ASSERT_TRUE(table->Put(d1, 9527, db, 0));
    std::string d2 = "d2";
    ASSERT_TRUE(table->Put(d2, 9527, db, 1));
    std::string d3 = "d3";
    ASSERT_TRUE(table->Put(d3, 9527, db, 2));
}*/
/* No need to test for ondisktable
TEST_F(DiskTableTest, Release) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 10);
    table->Init();
    table->Put("test", 9537, "test", 4);
    table->Put("test2", 9537, "test", 4);
    uint64_t cnt = table->Release();
    ASSERT_EQ(cnt, 2);
    delete table;
}*/
/* No need to test for ondisktable
TEST_F(DiskTableTest, IsExpired) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    // table ttl is 1
    Table* table = new Table("tx_log", 1, 1, 8, mapping, 1);
    table->Init();
    uint64_t now_time = ::baidu::common::timer::get_micros() / 1000;
    ::rtidb::api::LogEntry entry;
    uint64_t ts_time = now_time; 
    entry.set_ts(ts_time);
    ASSERT_FALSE(entry.ts() < table->GetExpireTime());
    
    // ttl_offset_ is 60 * 1000
    ts_time = now_time - 4 * 60 * 1000; 
    entry.set_ts(ts_time);
    ASSERT_TRUE(entry.ts() < table->GetExpireTime());
    delete table;
}*/

/*TEST_F(DiskTableTest, Iterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0|6", 0));
    DiskTable* table =
            new DiskTable("yjtable3", "", 1, 1, 2, "[SSD]/tmp/yjtest/yjssd1/|[SSD]/tmp/yjtest/yjssd2/", mapping, 10);
    table->Init();
    table->CreateTable();
    table->Put("yjtest", 9527, "test", 4);
    PDLOG(INFO, "Putted, yjtest 9527");
    table->Put("yjtest1", 9527, "test", 4);
    PDLOG(INFO, "Putted, yjtest1 9527");
    table->Put("yjtest", 9528, "test0", 5);
    PDLOG(INFO, "Putted, yjtest 9528");
    rocksdb::Iterator* it = table->NewIterator("yjtest");
    DiskTable::SeekTSWithKeyFromItr("yjtest", 9528, it);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("test0", it->value().ToString());
    ASSERT_EQ(5, it->value().size());
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("test", it->value().ToString());
    ASSERT_EQ(4, it->value().size());
    delete it;
    delete table;
}*/

//TEST_F(DiskTableTest, Iterator_GetSize) {
//    std::map<std::string, uint32_t> mapping;
//    mapping.insert(std::make_pair("idx0|6", 0));
//    DiskTable* table = new DiskTable("yjtable4", 1, 1, 8, mapping, 10);
//    table->Init();
//    table->CreateWithPath("/tmp/yjtest/yjtable4");
//    PDLOG(INFO, "CreateWithPath yjtable4");
//
//    table->Put("yjtest", 9527, "test", 4);
//    table->Put("yjtest", 9527, "test", 4);
//    table->Put("yjtest", 9528, "test0", 5);
//    rocksdb::Iterator* it = table->NewIterator("yjtest");
////    ASSERT_EQ(3, it->GetSize());
//    DiskTable::SeekTSWithKeyFromItr("yjtest", 9528, it);
//    ASSERT_TRUE(it->Valid());
//    ASSERT_EQ("test0", it->value().ToString());
//    ASSERT_EQ(5, it->value().size());
//    it->Next();
//    ASSERT_EQ("test", it->value().ToString());
//    ASSERT_EQ(4, it->value().size());
//    it->Next();
//    ASSERT_FALSE(it->Valid());
//    delete it;
//    delete table;
//}


/*
TEST_F(DiskTableTest, SchedGcForMultiDimissionTable) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();
    ASSERT_EQ(3, table->GetIdxCnt());
    DataBlock* db = new DataBlock(3, "helloworld", 10);
    std::string d1 = "d1";
    ASSERT_FALSE(table->Put(d1, 9527, db, 3));
    ASSERT_TRUE(table->Put(d1, 9527, db, 0));
    std::string d2 = "d2";
    ASSERT_TRUE(table->Put(d2, 9527, db, 1));
    std::string d3 = "d3";
    ASSERT_TRUE(table->Put(d3, 9527, db, 2));
    table->RecordCntIncr(1);
    ASSERT_EQ(3, table->GetRecordIdxCnt());
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(0, table->GetRecordCnt());
    ASSERT_EQ(0, table->GetRecordIdxCnt());
}

TEST_F(DiskTableTest, SchedGcHead) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->SetTTLType(::rtidb::api::TTLType::kLatestTime);
    table->Init();
    table->Put("test", 2, "test1", 5);
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    table->Put("test", 1, "test2", 5);
    ASSERT_EQ(2, table->GetRecordCnt());
    ASSERT_EQ(2, table->GetRecordIdxCnt());
    ASSERT_EQ(1, table->GetRecordPkCnt());
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(1, table->GetRecordCnt());
    ASSERT_EQ(1, table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());
}

TEST_F(DiskTableTest, SchedGcHead1) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    uint64_t keep_cnt = 500;
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, keep_cnt);
    table->SetTTLType(::rtidb::api::TTLType::kLatestTime);
    table->Init();
	uint64_t ts = 0;
    for (int i = 0; i < 10; i++) {
        int count = 5000;
        while (count) {
            ts++;
            table->Put("test", ts, "test1", 5);
            count--;
        }
        table->SchedGc();
        Ticket ticket;
        Iterator* it = table->NewIterator("test", ticket);

        it->Seek(ts + 1);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt / 2);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt / 4);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt + 1);
        ASSERT_TRUE(it->Valid());
        it->Seek(ts - keep_cnt);
        ASSERT_FALSE(it->Valid());
        it->Seek(ts - keep_cnt - 1);
        ASSERT_FALSE(it->Valid());
        delete it;
    }
    table->SchedGc();
}

TEST_F(DiskTableTest, SchedGc) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();

    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", now, "tes2", 4);
    uint64_t bytes = table->GetRecordByteSize();
    uint64_t record_idx_bytes = table->GetRecordIdxByteSize();
    table->Put("test", 9527, "test", 4);
    ASSERT_EQ(2, table->GetRecordCnt());
    ASSERT_EQ(2, table->GetRecordIdxCnt());
    ASSERT_EQ(1, table->GetRecordPkCnt());

    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(1, table->GetRecordCnt());
    ASSERT_EQ(1, table->GetRecordIdxCnt());
    ASSERT_EQ(bytes, table->GetRecordByteSize());
    ASSERT_EQ(record_idx_bytes, table->GetRecordIdxByteSize());

    Ticket ticket;
    Iterator* it = table->NewIterator("test", ticket);
    it->Seek(now);
    ASSERT_TRUE(it->Valid());
    std::string value_str(it->GetValue()->data, it->GetValue()->size);
    ASSERT_EQ("tes2", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete table;
}

TEST_F(DiskTableTest, OffSet) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->SetTimeOffset(-60 * 4);
    table->Put("test", now - 10 * 60 * 1000, "test", 4);
    table->Put("test", now - 3 * 60 * 1000, "test", 4);
    table->Put("test", now, "tes2", 4);
    table->Put("test", now + 3 * 60 * 1000, "tes2", 4);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);

    table->SetTimeOffset(0);
    table->SetExpire(false);
    count = table->SchedGc();
    ASSERT_EQ(0, count);
    table->SetExpire(true);
    count = table->SchedGc();
    ASSERT_EQ(1, count);
    {
        Ticket ticket;
        Iterator* it = table->NewIterator("test", ticket);
        it->Seek(now);
        ASSERT_TRUE(it->Valid());
        std::string value_str(it->GetValue()->data, it->GetValue()->size);
        ASSERT_EQ("tes2", value_str);
        it->Next();
        ASSERT_FALSE(it->Valid());
        delete it;
    }
    
    ASSERT_EQ(table->GetRecordCnt(), 2);
    ASSERT_EQ(table->GetRecordIdxCnt(), 2);
    table->SetTimeOffset(120);
    count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetRecordCnt(), 1);
    ASSERT_EQ(table->GetRecordIdxCnt(), 1);
    delete table;
}

TEST_F(DiskTableTest, TableDataCnt) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1, 8 , mapping, 1);
    table->Init();
    ASSERT_EQ(table->GetRecordCnt(), 0);
    uint64_t now = ::baidu::common::timer::get_micros() / 1000;
    table->Put("test", 9527, "test", 4);
    table->Put("test", now, "tes2", 4);
    ASSERT_EQ(table->GetRecordCnt(), 2);
    ASSERT_EQ(table->GetRecordIdxCnt(), 2);
    uint64_t count = table->SchedGc();
    ASSERT_EQ(1, count);
    ASSERT_EQ(table->GetRecordCnt(), 1);
    ASSERT_EQ(table->GetRecordIdxCnt(), 1);
    delete table;
}

TEST_F(DiskTableTest, TableUnref) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    Table* table = new Table("tx_log", 1, 1 ,8 , mapping, 1);
    table->Init();
    table->Put("test", 9527, "test", 4);
    delete table;
}
*/
}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    return RUN_ALL_TESTS();
}




