//
// codec_bench_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-12
//

#include "base/codec.h"
#include "storage/segment.h"
#include "gtest/gtest.h"
#include "proto/kv_pair.pb.h"
#include "proto/tablet.pb.h"
#include "base/kv_iterator.h"
#include "timer.h"
#include <iostream>

namespace rtidb {
namespace base {

class CodecBenchmarkTest : public ::testing::Test {

public:
    CodecBenchmarkTest(){}
    ~CodecBenchmarkTest() {}

};

TEST_F(CodecBenchmarkTest, Encode) {
    std::vector<::rtidb::storage::DataBlock*> data;
    char* bd = new char[400];
    for (uint32_t i = 0; i < 400; i++) {
        bd[i] = 'a';
    }

    for (uint32_t i = 0; i < 1000; i++) {
        ::rtidb::storage::DataBlock* block = new ::rtidb::storage::DataBlock(1, bd, 400);
        data.push_back(block);
    }

    //
    // test codec
    uint64_t time = 9527;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    for (uint32_t i = 0; i < 10000; i++) {
        char buffer[400 * 1000 + 1000 * 12];
        uint32_t offset = 0;
        for (uint32_t j = 0; j < 1000; j++) {
            ::rtidb::base::Encode(time, data[j], buffer, offset);
            offset += (4 + 8 + 400);
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;

    uint64_t pconsumed = ::baidu::common::timer::get_micros();

    for (uint32_t i = 0; i < 10000; i++) {
        ::rtidb::api::KvList list;
        for (uint32_t j = 0; j < 1000; j++) {
            ::rtidb::api::KvPair* pair = list.add_pairs();
            pair->set_time(time);
            pair->set_value(bd, 400);
        }
        std::string result;
        list.SerializeToString(&result);
    }

    pconsumed = ::baidu::common::timer::get_micros() - pconsumed;
    std::cout << "Encode rtidb: " << consumed/1000 << std::endl;
    std::cout << "Encode protobuf: " << pconsumed/1000 << std::endl;
}

TEST_F(CodecBenchmarkTest, Decode) {
    std::vector<::rtidb::storage::DataBlock*> data;
    char* bd = new char[400];

    for (uint32_t i = 0; i < 400; i++) {
        bd[i] = 'a';
    }

    for (uint32_t i = 0; i < 1000; i++) {
        ::rtidb::storage::DataBlock* block = new ::rtidb::storage::DataBlock(1, bd, 400);
        data.push_back(block);
    }
    char buffer[400 * 1000 + 1000 * 12];
    uint32_t offset = 0;
    uint64_t time = 9527;
    for (uint32_t j = 0; j < 1000; j++) {
        ::rtidb::base::Encode(time, data[j], buffer, offset);
        offset += (4 + 8 + 400);
    }

    ::rtidb::api::ScanResponse response;
    response.set_pairs(buffer, 412* 1000);

    uint64_t consumed = ::baidu::common::timer::get_micros();
    for (uint32_t i = 0; i < 10000;i++) {
        ::rtidb::base::KvIterator it(&response, false);
        while (it.Valid()) {
            it.Next();
            std::string value = it.GetValue().ToString();
            value.size();
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    ::rtidb::api::KvList list;
    for (uint32_t j = 0; j < 1000; j++) {
        ::rtidb::api::KvPair* pair = list.add_pairs();
        pair->set_time(time);
        pair->set_value(bd, 400);
    }
    std::string result;
    list.SerializeToString(&result);
    uint64_t pconsumed = ::baidu::common::timer::get_micros();
    for (uint32_t i = 0; i < 10000; i++) {
        ::rtidb::api::KvList kv_list;
        kv_list.ParseFromString(result);
    }

    pconsumed = ::baidu::common::timer::get_micros() - pconsumed;
    std::cout << "Decode rtidb: " << consumed/1000 << std::endl;
    std::cout << "Decode protobuf: " << pconsumed/1000 << std::endl;

}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

