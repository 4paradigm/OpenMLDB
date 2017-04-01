//
// segment_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include <iostream>
#include "storage/segment.h"
#include "gtest/gtest.h"
#include "gperftools/malloc_extension.h"

namespace rtidb {
namespace storage {

class SegmentTest : public ::testing::Test {

public:
    SegmentTest(){}
    ~SegmentTest() {}
};

TEST_F(SegmentTest, PutAndGet) {
   Segment segment; 
   const char* test = "test";
   std::string pk = "pk";
   segment.Put(pk, 9768, test, 4);
   DataBlock* db = NULL;
   bool ret = segment.Get(pk, 9768, &db);
   ASSERT_TRUE(ret);
   ASSERT_TRUE(db != NULL);
   ASSERT_EQ(4, db->size);
   std::string t(db->data);
   std::string e = "test";
   ASSERT_EQ(e, t);
}

TEST_F(SegmentTest, Iterator) {
   Segment segment; 
   segment.Put("pk", 9768, "test1", 5);
   segment.Put("pk", 9769, "test2", 5);
   Segment::Iterator* it = segment.NewIterator("pk");
   it->Seek(9768);
   ASSERT_EQ(9768, it->GetKey());
   DataBlock* value = it->GetValue();
   std::string result(value->data, value->size);
   ASSERT_EQ("test1", result);
   it->Next();
   value = it->GetValue();
   std::string result2(value->data, value->size);
   ASSERT_EQ("test2", result2);
   it->Next();
   ASSERT_FALSE(it->Valid());
   size_t allocated = 0;
   char data[400];
   for (uint32_t i = 0; i < 50000; i++) {
       std::string pk = "pk" + i;
       segment.Put(pk, i, data, 400);
   }
   MallocExtension* extension = MallocExtension::instance();
   char stat[2000];
   extension->GetStats(stat, 2000);
   std::string stat_str(stat);
   std::cout << stat_str << std::endl;
   extension->GetNumericProperty("generic.current_allocated_bytes", &allocated);
   std::cout << allocated << std::endl;


}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


