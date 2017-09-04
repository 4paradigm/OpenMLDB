//
// zk_client_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//

#include "zk/zk_client.h"
#include <gtest/gtest.h>

namespace rtidb {
namespace zk {


class ZkClientTest : public ::testing::Test {

public:
    ZkClientTest() {}

    ~ZkClientTest() {}
};

TEST_F(ZkClientTest, Init) {
    ZkClient client("127.0.0.1:2181", 1000, "127.0.0.1:9527", "/rtidb");
    bool ok = client.Init();
    ASSERT_TRUE(ok);
}
}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

