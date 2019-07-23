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
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD);
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
    RemoveData(path);
}

TEST_F(DiskTableTest, MultiDimensionPut) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable("yjtable2", 1, 2, mapping, 10,
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD);
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
    RemoveData(path);
}

TEST_F(DiskTableTest, Delete) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    mapping.insert(std::make_pair("idx1", 1));
    mapping.insert(std::make_pair("idx2", 2));
    DiskTable* table = new DiskTable("yjtable2", 1, 2, mapping, 10,
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD);
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
    RemoveData(path);
}

TEST_F(DiskTableTest, TraverseIterator) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 3, mapping, 0,
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD);
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
            ::rtidb::api::TTLType::kLatestTime, ::rtidb::common::StorageMode::kHDD);
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
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD);
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

    table = new DiskTable("t1", 1, 1, mapping, 10, 
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD);
    ASSERT_TRUE(table->LoadTable());
    raw_key = "test35";
    it = table->NewIterator(raw_key);
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
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD);
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

TEST_F(DiskTableTest, GcHead) {
    std::map<std::string, uint32_t> mapping;
    mapping.insert(std::make_pair("idx0", 0));
    DiskTable* table = new DiskTable("t1", 1, 3, mapping, 3,
            ::rtidb::api::TTLType::kLatestTime, ::rtidb::common::StorageMode::kHDD);
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
            ::rtidb::api::TTLType::kAbsoluteTime, ::rtidb::common::StorageMode::kHDD);
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

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    FLAGS_hdd_root_path = "/tmp/" + std::to_string(::rtidb::storage::GenRand());
    return RUN_ALL_TESTS();
}
