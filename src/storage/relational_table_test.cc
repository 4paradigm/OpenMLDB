//
// table_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/table.h"
#include "gtest/gtest.h"
#include "timer.h"
#ifdef TCMALLOC_ENABLE
#endif

namespace rtidb {
namespace storage {

TEST_F(RelationTableTest, Iterator) {
    // TODO(kongquan): iterator test not complete
    ASSERT_TRUE(true);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}




