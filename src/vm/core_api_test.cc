/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#include "vm/core_api.h"
#include "gtest/gtest.h"

namespace fesql {
namespace vm {

class CoreAPITest : public ::testing::Test {
 public:
    CoreAPITest() {}
    ~CoreAPITest() {}
};

TEST_F(CoreAPITest, test_create_new_row) {
    Schema schema;
    ::fesql::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::fesql::type::kInt16);
    col = schema.Add();
    col->set_name("col2");
    col->set_type(::fesql::type::kBool);
    codec::RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(0);

    fesql::codec::Row rowPtr = CoreAPI::NewRow(size);
    int8_t* buf = rowPtr.buf(0);
    builder.SetBuffer(buf, size);

    ASSERT_TRUE(builder.AppendNULL());
    ASSERT_TRUE(builder.AppendBool(false));
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
