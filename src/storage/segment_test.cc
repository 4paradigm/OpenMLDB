//
// segment_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/segment.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace storage {

class SegmentTest : public ::testing::Test {

public:
    SegmentTest(){}
    ~SegmentTest() {}
};

TEST_F(SegmentTest, Put0) {
   Segment segment(); 
   //const char* test = "test";
   //std::string pk = "pk";
   //segment.Put(pk, 9768, test, 5);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


