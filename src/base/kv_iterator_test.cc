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

#include "base/kv_iterator.h"

#include <iostream>

#include "base/strings.h"
#include "codec/row_codec.h"
#include "gtest/gtest.h"
#include "proto/tablet.pb.h"
#include "storage/segment.h"

namespace openmldb {
namespace base {

class KvIteratorTest : public ::testing::Test {
 public:
    KvIteratorTest() {}
    ~KvIteratorTest() {}
};

TEST_F(KvIteratorTest, Iterator_NULL) {
    ::openmldb::api::ScanResponse* response = new ::openmldb::api::ScanResponse();
    KvIterator kv_it(response);
    ASSERT_FALSE(kv_it.Valid());
}

TEST_F(KvIteratorTest, Iterator_ONE) {
    ::openmldb::api::ScanResponse* response = new ::openmldb::api::ScanResponse();
    std::string* pairs = response->mutable_pairs();
    pairs->resize(17);
    char* data = reinterpret_cast<char*>(&((*pairs)[0]));
    ::openmldb::storage::DataBlock* db1 = new ::openmldb::storage::DataBlock(1, "hello", 5);
    ::openmldb::codec::Encode(9527, db1, data, 0);
    KvIterator kv_it(response);
    ASSERT_TRUE(kv_it.Valid());
    ASSERT_EQ(9527, (int64_t)(kv_it.GetKey()));
    ASSERT_EQ("hello", kv_it.GetValue().ToString());
    kv_it.Next();
    ASSERT_FALSE(kv_it.Valid());
}

TEST_F(KvIteratorTest, Iterator) {
    ::openmldb::api::ScanResponse* response = new ::openmldb::api::ScanResponse();

    std::string* pairs = response->mutable_pairs();
    pairs->resize(34);
    char* data = reinterpret_cast<char*>(&((*pairs)[0]));
    ::openmldb::storage::DataBlock* db1 = new ::openmldb::storage::DataBlock(1, "hello", 5);
    ::openmldb::storage::DataBlock* db2 = new ::openmldb::storage::DataBlock(1, "hell1", 5);
    ::openmldb::codec::Encode(9527, db1, data, 0);
    ::openmldb::codec::Encode(9528, db2, data, 17);
    KvIterator kv_it(response);
    ASSERT_TRUE(kv_it.Valid());
    ASSERT_EQ(9527, (signed)kv_it.GetKey());
    ASSERT_EQ("hello", kv_it.GetValue().ToString());
    kv_it.Next();
    ASSERT_TRUE(kv_it.Valid());
    ASSERT_EQ(9528, (signed)kv_it.GetKey());
    ASSERT_EQ("hell1", kv_it.GetValue().ToString());
    kv_it.Next();
    ASSERT_FALSE(kv_it.Valid());
}

TEST_F(KvIteratorTest, HasPK) {
    ::openmldb::api::TraverseResponse* response = new ::openmldb::api::TraverseResponse();

    std::string* pairs = response->mutable_pairs();
    pairs->resize(52);
    char* data = reinterpret_cast<char*>(&((*pairs)[0]));
    ::openmldb::storage::DataBlock* db1 = new ::openmldb::storage::DataBlock(1, "hello", 5);
    ::openmldb::storage::DataBlock* db2 = new ::openmldb::storage::DataBlock(1, "hell1", 5);
    ::openmldb::codec::EncodeFull("test1", 9527, db1, data, 0);
    ::openmldb::codec::EncodeFull("test2", 9528, db2, data, 26);
    KvIterator kv_it(response);
    ASSERT_TRUE(kv_it.Valid());
    ASSERT_STREQ("test1", kv_it.GetPK().c_str());
    ASSERT_EQ(9527, (signed)kv_it.GetKey());
    ASSERT_STREQ("hello", kv_it.GetValue().ToString().c_str());
    kv_it.Next();
    ASSERT_TRUE(kv_it.Valid());
    ASSERT_STREQ("test2", kv_it.GetPK().c_str());
    ASSERT_EQ(9528, (signed)kv_it.GetKey());
    ASSERT_STREQ("hell1", kv_it.GetValue().ToString().c_str());
    kv_it.Next();
    ASSERT_FALSE(kv_it.Valid());
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
