/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>

#include "base/kv_iterator.h"
#include "codec/row_codec.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "storage/segment.h"

namespace openmldb {
namespace codec {

class CodecBenchmarkTest : public ::testing::Test {
 public:
    CodecBenchmarkTest() {}
    ~CodecBenchmarkTest() {}
};

void RunHasTs(::openmldb::storage::DataBlock* db) {
    boost::container::deque<std::pair<uint64_t, ::openmldb::base::Slice>> datas;
    uint32_t total_block_size = 0;
    for (uint32_t i = 0; i < 1000; i++) {
        datas.emplace_back(1000, std::move(::openmldb::base::Slice(db->data, db->size)));
        total_block_size += db->size;
    }
    std::string pairs;
    ::openmldb::codec::EncodeRows(datas, total_block_size, &pairs);
}

void RunNoneTs(::openmldb::storage::DataBlock* db) {
    std::vector<::openmldb::base::Slice> datas;
    datas.reserve(1000);
    uint32_t total_block_size = 0;
    for (uint32_t i = 0; i < 1000; i++) {
        datas.push_back(::openmldb::base::Slice(db->data, db->size));
        total_block_size += db->size;
    }
    std::string pairs;
    ::openmldb::codec::EncodeRows(datas, total_block_size, &pairs);
}

TEST_F(CodecBenchmarkTest, ProjectTest) {
    Schema schema;
    for (uint32_t i = 0; i < 100; i++) {
        common::ColumnDesc* col = schema.Add();
        col->set_name("col" + std::to_string(i));
        col->set_data_type(type::kBigInt);
    }
    common::ColumnDesc* col_last = schema.Add();
    col_last->set_name("col_last");
    col_last->set_data_type(type::kVarchar);
    std::string hello = "hello";
    RowBuilder rb(schema);
    uint32_t total_size = rb.CalTotalLength(hello.size());
    void* ptr = ::malloc(total_size);
    rb.SetBuffer(reinterpret_cast<int8_t*>(ptr), total_size);
    for (uint32_t i = 0; i < 100; i++) {
        int64_t val = 100;
        (void)rb.AppendInt64(val);
    }
    (void)rb.AppendString(hello.c_str(), hello.size());
    ProjectList plist;
    uint32_t* idx = plist.Add();
    *idx = 100;
    uint32_t* idx2 = plist.Add();
    *idx2 = 99;

    uint64_t consumed = ::baidu::common::timer::get_micros();
    std::map<int32_t, std::shared_ptr<Schema>> vers_schema;
    vers_schema.insert(std::make_pair(1, std::make_shared<Schema>(schema)));
    for (int64_t i = 1; i < 100; i++) {
        RowProject rp(vers_schema, plist);
        rp.Init();
        for (int32_t j = 0; j < 1000; j++) {
            int8_t* data = NULL;
            uint32_t size = 0;
            rp.Project(reinterpret_cast<int8_t*>(ptr), total_size, &data, &size);
            free(reinterpret_cast<void*>(data));
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    std::cout << "project 1000 records avg consumed:" << consumed / 100 << "μs" << std::endl;
}

TEST_F(CodecBenchmarkTest, Encode_ts_vs_none_ts) {
    char* bd = new char[128];
    for (uint32_t i = 0; i < 128; i++) {
        bd[i] = 'a';
    }
    ::openmldb::storage::DataBlock* block = new ::openmldb::storage::DataBlock(1, bd, 128);
    for (uint32_t i = 0; i < 10; i++) {
        RunHasTs(block);
        RunNoneTs(block);
    }

    uint64_t consumed = ::baidu::common::timer::get_micros();

    for (uint32_t i = 0; i < 10000; i++) {
        RunHasTs(block);
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;

    uint64_t pconsumed = ::baidu::common::timer::get_micros();
    for (uint32_t i = 0; i < 10000; i++) {
        RunNoneTs(block);
    }
    pconsumed = ::baidu::common::timer::get_micros() - pconsumed;
    std::cout << "encode 1000 records has ts avg consumed:" << consumed / 10000 << "μs" << std::endl;
    std::cout << "encode 1000 records has no ts avg consumed " << pconsumed / 10000 << "μs" << std::endl;
}

TEST_F(CodecBenchmarkTest, Encode) {
    std::vector<::openmldb::storage::DataBlock*> data;
    char* bd = new char[400];
    for (uint32_t i = 0; i < 400; i++) {
        bd[i] = 'a';
    }

    for (uint32_t i = 0; i < 1000; i++) {
        ::openmldb::storage::DataBlock* block = new ::openmldb::storage::DataBlock(1, bd, 400);
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
            ::openmldb::codec::Encode(time, data[j], buffer, offset);
            offset += (4 + 8 + 400);
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;

    uint64_t pconsumed = ::baidu::common::timer::get_micros();

    for (uint32_t i = 0; i < 10000; i++) {
        ::openmldb::common::KvList list;
        for (uint32_t j = 0; j < 1000; j++) {
            ::openmldb::common::KvPair* pair = list.add_pairs();
            pair->set_time(time);
            pair->set_value(bd, 400);
        }
        std::string result;
        list.SerializeToString(&result);
    }

    pconsumed = ::baidu::common::timer::get_micros() - pconsumed;
    std::cout << "Encode openmldb: " << consumed / 1000 << std::endl;
    std::cout << "Encode protobuf: " << pconsumed / 1000 << std::endl;
}

TEST_F(CodecBenchmarkTest, Decode) {
    std::vector<::openmldb::storage::DataBlock*> data;
    char* bd = new char[400];

    for (uint32_t i = 0; i < 400; i++) {
        bd[i] = 'a';
    }

    for (uint32_t i = 0; i < 1000; i++) {
        ::openmldb::storage::DataBlock* block = new ::openmldb::storage::DataBlock(1, bd, 400);
        data.push_back(block);
    }
    char buffer[400 * 1000 + 1000 * 12];
    uint32_t offset = 0;
    uint64_t time = 9527;
    for (uint32_t j = 0; j < 1000; j++) {
        ::openmldb::codec::Encode(time, data[j], buffer, offset);
        offset += (4 + 8 + 400);
    }

    auto response = std::make_shared<::openmldb::api::ScanResponse>();
    response->set_pairs(buffer, 412 * 1000);

    uint64_t consumed = ::baidu::common::timer::get_micros();
    for (uint32_t i = 0; i < 10000; i++) {
        ::openmldb::base::ScanKvIterator it("", response);
        while (it.Valid()) {
            it.Next();
            std::string value = it.GetValue().ToString();
            value.size();
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    ::openmldb::common::KvList list;
    for (uint32_t j = 0; j < 1000; j++) {
        ::openmldb::common::KvPair* pair = list.add_pairs();
        pair->set_time(time);
        pair->set_value(bd, 400);
    }
    std::string result;
    list.SerializeToString(&result);
    uint64_t pconsumed = ::baidu::common::timer::get_micros();
    for (uint32_t i = 0; i < 10000; i++) {
        ::openmldb::common::KvList kv_list;
        kv_list.ParseFromString(result);
    }

    pconsumed = ::baidu::common::timer::get_micros() - pconsumed;
    std::cout << "Decode openmldb: " << consumed / 1000 << std::endl;
    std::cout << "Decode protobuf: " << pconsumed / 1000 << std::endl;
}

}  // namespace codec
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
