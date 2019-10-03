//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author yangjun
// Date 2018-01-07
//

#include "storage/disk_table.h"
#include "gtest/gtest.h"
#include "timer.h"
#include "logging.h"
#include "base/file_util.h"

using ::baidu::common::INFO;

DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);

namespace rtidb {
namespace storage {

inline uint32_t GenRand() {
    srand((unsigned)time(NULL));
    return rand() % 10000000 + 1;
}

void RemoveData(const std::string& path) {
    ::rtidb::base::RemoveDir(path+"/data");
    ::rtidb::base::RemoveDir(path);
    ::rtidb::base::RemoveDir(FLAGS_hdd_root_path);
}

class DiskTableTest : public ::testing::Test {

public:
    DiskTableTest() {}
    ~DiskTableTest() {}
};

TEST_F(DiskTableTest, ParseKeyAndTs) {
    std::string combined_key = CombineKeyTs("abcdexxx11",1552619498000);
    std::string key;
    uint64_t ts;
    ASSERT_EQ(0, ParseKeyAndTs(combined_key, key, ts));
    ASSERT_EQ("abcdexxx11", key);
    ASSERT_EQ(1552619498000, ts);
    combined_key = CombineKeyTs("abcdexxx11", 1);
    ASSERT_EQ(0, ParseKeyAndTs(combined_key, key, ts));
    ASSERT_EQ("abcdexxx11", key);
    ASSERT_EQ(1, ts);
    combined_key = CombineKeyTs("0", 0);
    ASSERT_EQ(0, ParseKeyAndTs(combined_key, key, ts));
    ASSERT_EQ("0", key);
    ASSERT_EQ(0, ts);
    ASSERT_EQ(-1, ParseKeyAndTs("abc", key, ts));
    combined_key = CombineKeyTs("", 1122);
    ASSERT_EQ(0, ParseKeyAndTs(combined_key, key, ts));
    ASSERT_TRUE(key.empty());
    ASSERT_EQ(1122, ts);
}

TEST_F(DiskTableTest, Put) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("yjtable1", 1, 1, mapping, 10, 
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    std::string raw_key = "test35";
    Ticket ticket;
    TableIterator* it = table->NewIterator(raw_key, ticket);
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
    RemoveData(path);
}

TEST_F(DiskTableTest, MultiDimensionPut) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable("yjtable2", 1, 2, mapping, 10,
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
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
    Ticket ticket;
    TableIterator* it = table->NewIterator(0, "yjdim0", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    uint64_t ts = it->GetKey();
    ASSERT_EQ(1, ts);
    std::string value1 = it->GetValue().ToString();
    ASSERT_EQ("yjtestvalue", value1);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

    it = table->NewIterator(1, "yjdim1", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    value1 = it->GetValue().ToString();
    ASSERT_EQ("yjtestvalue", value1);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

    it = table->NewIterator(2, "yjdim2", ticket);
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

    it = table->NewIterator(0, "key2", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1", ticket);
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    delete it;

    std::string val;
    ASSERT_TRUE(table->Get(1, "key1", 2, val));
    ASSERT_EQ("value2", val);


    it = table->NewIterator(2, "dimxxx1", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1", ticket);
    it->Seek(2);
    ASSERT_TRUE(it->Valid());
    ts = it->GetKey();
    ASSERT_EQ(2, ts);
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    it = table->NewIterator(1, "key1", ticket);
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2, it->GetKey());
    value1 = it->GetValue().ToString();
    ASSERT_EQ("value2", value1);
    delete it;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_2";
    RemoveData(path);
}

TEST_F(DiskTableTest, Delete) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable("yjtable2", 1, 2, mapping, 10,
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 10; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    Ticket ticket;
    TableIterator* it = table->NewIterator("test6", ticket);
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
    it = table->NewIterator("test6", ticket);
    it->SeekToFirst();
    ASSERT_FALSE(it->Valid());
    delete it;

    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_2";
    RemoveData(path);
}

TEST_F(DiskTableTest, TraverseIterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 3, mapping, 0,
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
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
    RemoveData(path);
}

TEST_F(DiskTableTest, TraverseIteratorLatest) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 3, mapping, 3,
            ::rtidb::api::TTLType::kLatestTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
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
    RemoveData(path);
}

TEST_F(DiskTableTest, Load) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 1, mapping, 10, 
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    std::string raw_key = "test35";
    Ticket ticket;
    TableIterator* it = table->NewIterator(raw_key, ticket);
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

    table = new DiskTable("t1", 1, 1, mapping, 10, 
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->LoadTable());
    raw_key = "test35";
    it = table->NewIterator(raw_key, ticket);
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
    RemoveData(path);
}

TEST_F(DiskTableTest, CompactFilter) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 1, mapping, 10, 
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            if (k > 2) {
                ASSERT_TRUE(table->Put(key, ts - k - 10 * 60 * 1000, "value9", 6));
            } else {
                ASSERT_TRUE(table->Put(key, ts - k, "value", 5));
            }
        }
    }
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            std::string value;
            if (k > 2) {
                ASSERT_TRUE(table->Get(key, ts - k - 10 * 60 * 1000, value));
                ASSERT_EQ("value9", value);
            } else {
                ASSERT_TRUE(table->Get(key, ts - k, value));
                ASSERT_EQ("value", value);
            }
        }
    }
    table->CompactDB();
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            std::string value;
            if (k > 2) {
                ASSERT_FALSE(table->Get(key, ts - k - 10 * 60 * 1000, value));
            } else {
                ASSERT_TRUE(table->Get(key, ts - k, value));
                ASSERT_EQ("value", value);
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_1";
    RemoveData(path);
}

TEST_F(DiskTableTest, CompactFilterMulTs) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_tid(1);
    table_meta.set_pid(0);
    table_meta.set_ttl(10);
    table_meta.set_storage_mode(::rtidb::common::kHDD);
    ::rtidb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_type("string");
    ::rtidb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_type("string");
    ::rtidb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_type("uint64");
    column_desc2->set_is_ts_col(true);
    column_desc2->set_ttl(3);
    ::rtidb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_type("uint64");
    column_desc3->set_is_ts_col(true);
    column_desc3->set_ttl(5);
    ::rtidb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_ts_name("ts1");
    column_key->add_ts_name("ts2");
    ::rtidb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc");
    column_key1->add_ts_name("ts2");

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    for (int idx = 0; idx < 100; idx++) {
        Dimensions dims;
        ::rtidb::api::Dimension* dim = dims.Add();
        dim->set_key("card" + std::to_string(idx));
        dim->set_idx(0);
        ::rtidb::api::Dimension* dim1 = dims.Add();
        dim1->set_key("mcc" + std::to_string(idx));
        dim1->set_idx(1);
        std::string key = "test" + std::to_string(idx);
        if (idx == 5 || idx == 10) {
            for (int i = 0; i < 10; i++) {
                TSDimensions ts_dims;
                ::rtidb::api::TSDimension* ts_dim = ts_dims.Add();
                ts_dim->set_ts(cur_time - i * 60 * 1000);
                ts_dim->set_idx(0);
                ::rtidb::api::TSDimension* ts_dim1 = ts_dims.Add();
                ts_dim1->set_ts(cur_time - i * 60 * 1000);
                ts_dim1->set_idx(1);
                ASSERT_TRUE(table->Put(dims, ts_dims, "value" + std::to_string(i)));
            }

        } else {
            for (int i = 0; i < 10; i++) {
                TSDimensions ts_dims;
                ::rtidb::api::TSDimension* ts_dim = ts_dims.Add();
                ts_dim->set_ts(cur_time - i);
                ts_dim->set_idx(0);
                ::rtidb::api::TSDimension* ts_dim1 = ts_dims.Add();
                ts_dim1->set_ts(cur_time - i);
                ts_dim1->set_idx(1);
                ASSERT_TRUE(table->Put(dims, ts_dims, "value" + std::to_string(i)));
            }
        }
    }
    Ticket ticket;
    TableIterator* iter = table->NewIterator(0, 0, "card0", ticket);
    iter->SeekToFirst();
    while(iter->Valid()) {
        //printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    iter = table->NewIterator(1, 1, "mcc0", ticket);
    iter->SeekToFirst();
    while(iter->Valid()) {
        //printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "card" + std::to_string(idx);
        std::string key1 = "mcc" + std::to_string(idx);
        uint64_t ts = cur_time;
        if (idx == 5 || idx == 10) {
            for (int i = 0; i < 10; i++) {
                std::string e_value = "value" + std::to_string(i);
                std::string value;
                ASSERT_TRUE(table->Get(0, key, ts - i * 60 * 1000, 0, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(0, key, ts - i * 60 * 1000, 1, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key1, ts - i * 60 * 1000, 1, value));
            }

        } else {
            for (int i = 0; i < 10; i++) {
                std::string e_value = "value" + std::to_string(i);
                std::string value;
                //printf("idx:%d i:%d key:%s ts:%lu\n", idx, i, key.c_str(), ts - i);
                //printf("idx:%d i:%d key:%s ts:%lu\n", idx, i, key1.c_str(), ts - i);
                ASSERT_TRUE(table->Get(0, key, ts - i, 0, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(0, key, ts - i, 1, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key1, ts - i, 1, value));
            }
        }
    }
    table->CompactDB();
    iter = table->NewIterator(0, 0, "card0", ticket);
    iter->SeekToFirst();
    while(iter->Valid()) {
        //printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    iter = table->NewIterator(1, 1, "mcc0", ticket);
    iter->SeekToFirst();
    while(iter->Valid()) {
        //printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "card" + std::to_string(idx);
        std::string key1 = "mcc" + std::to_string(idx);
        uint64_t ts = cur_time;
        if (idx == 5 || idx == 10) {
            for (int i = 0; i < 10; i++) {
                std::string e_value = "value" + std::to_string(i);
                std::string value;
                uint64_t cur_ts = ts - i * 60 * 1000;
                if (i < 3) {
                    ASSERT_TRUE(table->Get(0, key, cur_ts, 0, value));
                    ASSERT_EQ(e_value, value);
                } else {
                    ASSERT_FALSE(table->Get(0, key, cur_ts, 0, value));
                }
                if (i < 5) {
                    ASSERT_TRUE(table->Get(0, key, cur_ts, 1, value));
                    ASSERT_EQ(e_value, value);
                    ASSERT_TRUE(table->Get(1, key1, cur_ts, 1, value));
                } else {
                    //printf("idx:%lu i:%d key:%s ts:%lu\n", idx, i, key.c_str(), cur_ts);
                    ASSERT_FALSE(table->Get(0, key, cur_ts, 1, value));
                    //printf("idx:%lu i:%d key:%s ts:%lu\n", idx, i, key1.c_str(), cur_ts);
                    ASSERT_FALSE(table->Get(1, key1, cur_ts, 1, value));
                }
            }
        } else {
            for (int i = 0; i < 10; i++) {
                std::string e_value = "value" + std::to_string(i);
                std::string value;
                ASSERT_TRUE(table->Get(0, key, ts - i, 0, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(0, key, ts - i, 1, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key1, ts - i, 1, value));
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_0";
    RemoveData(path);
}

TEST_F(DiskTableTest, GcHeadMulTs) {
    ::rtidb::api::TableMeta table_meta;
    table_meta.set_tid(1);
    table_meta.set_pid(0);
    table_meta.set_ttl(10);
    table_meta.set_ttl_type(::rtidb::api::TTLType::kLatestTime);
    table_meta.set_storage_mode(::rtidb::common::kHDD);
    ::rtidb::common::ColumnDesc* column_desc = table_meta.add_column_desc();
    column_desc->set_name("card");
    column_desc->set_type("string");
    ::rtidb::common::ColumnDesc* column_desc1 = table_meta.add_column_desc();
    column_desc1->set_name("mcc");
    column_desc1->set_type("string");
    ::rtidb::common::ColumnDesc* column_desc2 = table_meta.add_column_desc();
    column_desc2->set_name("ts1");
    column_desc2->set_type("uint64");
    column_desc2->set_is_ts_col(true);
    column_desc2->set_ttl(3);
    ::rtidb::common::ColumnDesc* column_desc3 = table_meta.add_column_desc();
    column_desc3->set_name("ts2");
    column_desc3->set_type("uint64");
    column_desc3->set_is_ts_col(true);
    column_desc3->set_ttl(5);
    ::rtidb::common::ColumnKey* column_key = table_meta.add_column_key();
    column_key->set_index_name("card");
    column_key->add_ts_name("ts1");
    column_key->add_ts_name("ts2");
    ::rtidb::common::ColumnKey* column_key1 = table_meta.add_column_key();
    column_key1->set_index_name("mcc");
    column_key1->add_ts_name("ts2");

    DiskTable* table = new DiskTable(table_meta, FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    for (int idx = 0; idx < 100; idx++) {
        Dimensions dims;
        ::rtidb::api::Dimension* dim = dims.Add();
        dim->set_key("card" + std::to_string(idx));
        dim->set_idx(0);
        ::rtidb::api::Dimension* dim1 = dims.Add();
        dim1->set_key("mcc" + std::to_string(idx));
        dim1->set_idx(1);
        std::string key = "test" + std::to_string(idx);
        for (int i = 0; i < 10; i++) {
            if (idx == 50 && i > 2) {
                break;
            }
            TSDimensions ts_dims;
            ::rtidb::api::TSDimension* ts_dim = ts_dims.Add();
            ts_dim->set_ts(cur_time - i);
            ts_dim->set_idx(0);
            ::rtidb::api::TSDimension* ts_dim1 = ts_dims.Add();
            ts_dim1->set_ts(cur_time - i);
            ts_dim1->set_idx(1);
            ASSERT_TRUE(table->Put(dims, ts_dims, "value" + std::to_string(i)));
        }
    }
    Ticket ticket;
    TableIterator* iter = table->NewIterator(0, 0, "card0", ticket);
    iter->SeekToFirst();
    while(iter->Valid()) {
        //printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    iter = table->NewIterator(1, 1, "mcc0", ticket);
    iter->SeekToFirst();
    while(iter->Valid()) {
        //printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "card" + std::to_string(idx);
        std::string key1 = "mcc" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int i = 0; i < 10; i++) {
            std::string e_value = "value" + std::to_string(i);
            std::string value;
            //printf("idx:%d i:%d key:%s ts:%lu\n", idx, i, key.c_str(), ts - i);
            //printf("idx:%d i:%d key:%s ts:%lu\n", idx, i, key1.c_str(), ts - i);
            if (idx == 50 && i > 2) {
                ASSERT_FALSE(table->Get(0, key, ts - i, 0, value));
                ASSERT_FALSE(table->Get(0, key, ts - i, 1, value));
                ASSERT_FALSE(table->Get(1, key1, ts - i, 1, value));
            } else {
                ASSERT_TRUE(table->Get(0, key, ts - i, 0, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(0, key, ts - i, 1, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key1, ts - i, 1, value));
            }
        }
    }
    table->SchedGc();
    iter = table->NewIterator(0, 0, "card0", ticket);
    iter->SeekToFirst();
    while(iter->Valid()) {
        //printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    iter = table->NewIterator(1, 1, "mcc0", ticket);
    iter->SeekToFirst();
    while(iter->Valid()) {
        //printf("key %s ts %lu\n", iter->GetPK().c_str(), iter->GetKey());
        iter->Next();
    }
    delete iter;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "card" + std::to_string(idx);
        std::string key1 = "mcc" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int i = 0; i < 10; i++) {
            std::string e_value = "value" + std::to_string(i);
            std::string value;
            //printf("idx:%d, i:%d, key:%s, ts:%lu\n", idx, i , key.c_str(), ts-i);
            if (idx == 50 && i > 2) {
                ASSERT_FALSE(table->Get(0, key, ts - i, 0, value));
                ASSERT_FALSE(table->Get(0, key, ts - i, 1, value));
                ASSERT_FALSE(table->Get(1, key1, ts - i, 1, value));
            } else if (i < 3) {
                ASSERT_TRUE(table->Get(0, key, ts - i, 0, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(0, key, ts - i, 1, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key1, ts - i, 1, value));
            } else if (i < 5) {
                ASSERT_FALSE(table->Get(0, key, ts - i, 0, value));
                ASSERT_TRUE(table->Get(0, key, ts - i, 1, value));
                ASSERT_EQ(e_value, value);
                ASSERT_TRUE(table->Get(1, key1, ts - i, 1, value));
            } else {
                ASSERT_FALSE(table->Get(0, key, ts - i, 0, value));
                ASSERT_FALSE(table->Get(0, key, ts - i, 1, value));
                ASSERT_FALSE(table->Get(1, key1, ts - i, 1, value));
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_0";
    RemoveData(path);
}

TEST_F(DiskTableTest, GcHead) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 3, mapping, 3,
            ::rtidb::api::TTLType::kLatestTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 5; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
            if (idx == 10 && k == 2) {
                ASSERT_TRUE(table->Put(key, ts + k, "value9", 6));
                ASSERT_TRUE(table->Put(key, ts + k, "value8", 6));
            }
        }
    }
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 5; k++) {
            std::string value;
            ASSERT_TRUE(table->Get(key, ts + k, value));
            if (idx == 10 && k == 2) {
                ASSERT_EQ("value8", value);
            } else {
                ASSERT_EQ("value", value);
            }
        }
    }
    table->GcHead();
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 5; k++) {
            std::string value;
            if (k < 2) {
                ASSERT_FALSE(table->Get(key, ts + k, value));
            } else {
                ASSERT_TRUE(table->Get(key, ts + k, value));
                if (idx == 10 && k == 2) {
                    ASSERT_EQ("value8", value);
                } else {
                    ASSERT_EQ("value", value);
                }
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_3";
    RemoveData(path);
}

TEST_F(DiskTableTest, GcTTL) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 3, mapping, 10,
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            if (k > 2) {
                ASSERT_TRUE(table->Put(key, ts - k - 10 * 60 * 1000, "value9", 6));
            } else {
                ASSERT_TRUE(table->Put(key, ts - k, "value", 5));
            }
        }
    }
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            std::string value;
            if (k > 2) {
                ASSERT_TRUE(table->Get(key, ts - k - 10 * 60 * 1000, value));
                ASSERT_EQ("value9", value);
            } else {
                ASSERT_TRUE(table->Get(key, ts - k, value));
                ASSERT_EQ("value", value);
            }
        }
    }
    table->GcTTL();
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = cur_time;
        for (int k = 0; k < 5; k++) {
            std::string value;
            if (k > 2) {
                ASSERT_FALSE(table->Get(key, ts - k - 10 * 60 * 1000, value));
            } else {
                ASSERT_TRUE(table->Get(key, ts - k, value));
                ASSERT_EQ("value", value);
            }
        }
    }
    delete table;
    std::string path = FLAGS_hdd_root_path + "/1_3";
    RemoveData(path);
}

TEST_F(DiskTableTest, CheckPoint) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 1, mapping, 0, 
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->Init());
    for (int idx = 0; idx < 100; idx++) {
        std::string key = "test" + std::to_string(idx);
        uint64_t ts = 9537;
        for (int k = 0; k < 10; k++) {
            ASSERT_TRUE(table->Put(key, ts + k, "value", 5));
        }
    }
    std::string raw_key = "test35";
    Ticket ticket;
    TableIterator* it = table->NewIterator(raw_key, ticket);
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
    
    std::string snapshot_path = FLAGS_hdd_root_path + "/1_1/snapshot";
    ASSERT_EQ(table->CreateCheckPoint(snapshot_path), 0);
    delete table;

    std::string data_path = FLAGS_hdd_root_path + "/1_1/data";
    ::rtidb::base::RemoveDir(data_path);

    ::rtidb::base::Rename(snapshot_path, data_path);

    table = new DiskTable("t1", 1, 1, mapping, 0, 
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD,
            FLAGS_hdd_root_path);
    ASSERT_TRUE(table->LoadTable());
    raw_key = "test35";
    it = table->NewIterator(raw_key, ticket);
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

    it = table->NewTraverseIterator(0);
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::string pk = it->GetPK();;
        count++;
        it->Next();
    }
    ASSERT_FALSE(it->Valid());
    ASSERT_EQ(1000, count);
    delete it;
    delete table;

    std::string path = FLAGS_hdd_root_path + "/1_1";
    RemoveData(path);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    FLAGS_hdd_root_path = "/tmp/" + std::to_string(::rtidb::storage::GenRand());
    return RUN_ALL_TESTS();
}
