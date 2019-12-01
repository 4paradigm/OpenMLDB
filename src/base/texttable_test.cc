/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * texttable_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/12
 *--------------------------------------------------------------------------
 **/
#include "base/texttable.h"
#include "gtest/gtest.h"
namespace fesql {
namespace base {
class TextTableTest : public ::testing::Test {
 public:
    TextTableTest() {}
    ~TextTableTest() {}
};

TEST_F(TextTableTest, TextTableFormatTest) {
    base::TextTable t('-', '|', '+');
    t.add("Field");
    t.add("Type");
    t.add("NULL");
    t.endOfRow();

    t.add("column1");
    t.add("kInt32");
    t.add("No");
    t.endOfRow();

    t.add("column2");
    t.add("kInt64");
    t.add("YES");
    t.endOfRow();

    t.add("ts");
    t.add("kTimestamp");
    t.add("YES");
    t.endOfRow();
    std::cout << t << std::endl;
}
}  // namespace base
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
