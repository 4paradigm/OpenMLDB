//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/relational_table.h"
#include "gtest/gtest.h"
#include "timer.h"
#ifdef TCMALLOC_ENABLE
#endif

namespace rtidb {
namespace storage {

inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);
}

class RelationTableTest: public ::testing::Test {

public:
    RelationTableTest() {}
    ~RelationTableTest() {}
};


    TEST_F(RelationTableTest, Iterator) {
    uint32_t tid = rand() % 10000000 + 1, pid = 0;
    rtidb::api::TableMeta tableMeta;
    tableMeta.set_tid(tid);
    tableMeta.set_pid(pid);
    tableMeta.set_storage_mode(rtidb::common::kHDD);
    tableMeta.set_name("test-relation");
    tableMeta.set_mode(rtidb::api::TableMode::kTableLeader);
    std::vector<std::string> columns = {"card", "mcc"};
    std::string index_col = "card";
    for (const auto& col : columns) {
        auto column = tableMeta.add_column_desc();
        column->set_data_type(rtidb::type::kVarchar);
        column->set_name(col);
        if (col == index_col) {
            column->set_add_ts_idx(true);
        }
    }
    {
        auto column = tableMeta.add_added_column_desc();
        column->set_data_type(rtidb::type::kInt);
        column->set_name("p_biz_data");
    }
    auto table_key = tableMeta.add_column_key();
    table_key->set_index_name(index_col);
    table_key->add_col_name(index_col);
    table_key->set_index_type(rtidb::type::kPrimaryKey);
    std::string db_path = "/tmp/test" + GenRand();
    RelationalTable table(tableMeta, db_path);
    ASSERT_TRUE(table.Init());

    rtidb::base::RowBuilder rb(tableMeta.column_desc());
    for (int i = 0; i < 10000000; i++) {
        std::string card, mcc;
        std::string tmp = std::to_string(i);
        card = "card" + tmp;
        mcc = "mcc" + tmp;
        std::string row;
        uint32_t str_len = card.size() + mcc.size();
        uint32_t total_size = rb.CalTotalLength(str_len);
        row.resize(total_size);
        rb.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), total_size);
        rb.AppendString(card.data(), card.length());
        rb.AppendString(mcc.data(), mcc.length());
        rb.AppendInt32(i);
        ASSERT_TRUE(table.Put(row));
        auto iter = table.NewTraverse(0, 0);
        iter->SeekToFirst();
    }
    // TODO(kongquan): iterator test not complete
    std::cout << "put done" << std::endl;
    sleep(10);
    ASSERT_TRUE(true);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    return RUN_ALL_TESTS();
}




