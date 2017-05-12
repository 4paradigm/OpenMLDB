//
// tablet_impl_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-05
//

#include "tablet/table_ha.h"
#include "proto/tablet.pb.h"
#include "gtest/gtest.h"
#include "storage/table.h"
#include "logging.h"
#include "timer.h"

namespace rtidb {
namespace tablet {


class TableHaTest : public ::testing::Test {

public:
    TableHaTest() {}
    ~TableHaTest() {}
};


TEST_F(TableHaTest, All) {
    TableDataHA* ha = new TableDataHA("/tmp/ritdb/", "1024");
    ASSERT_EQ(true, ha->Init());
    ::rtidb::api::TableMeta meta;
    meta.set_tid(1024);
    meta.set_name("1024");
    meta.set_pid(0);
    meta.set_ttl(-1);
    meta.set_seg_cnt(8);
    ASSERT_EQ(true, ha->SaveMeta(meta));
    TableRow row;
    row.set_pk("test");
    row.set_data("9527");
    row.set_time(1000);
    ASSERT_EQ(true, ha->Put(row));
    ::rtidb::storage::Table* table = NULL;
    ASSERT_EQ(true, ha->Recover(&table));
    ASSERT_EQ(true, table != NULL);
    ASSERT_EQ(1024, table->GetId());
    ASSERT_EQ(0, table->GetPid());
    ASSERT_EQ(-1, table->GetTTL());
    ASSERT_EQ(8, table->GetSegCnt());

    ::rtidb::storage::Table::Iterator* it = table->NewIterator("test");
    it->Seek(1000);
    ASSERT_TRUE(it->Valid());
    ::rtidb::storage::DataBlock* value1 = it->GetValue();
    std::string value_str(value1->data, value1->size);
    ASSERT_EQ("9527", value_str);
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;

}



}
}

int main(int argc, char** argv) {
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



