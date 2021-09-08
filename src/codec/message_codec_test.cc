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

#include "codec/message_codec.h"

#include <iostream>

#include "gtest/gtest.h"

namespace openmldb {
namespace codec {

class MessageCodecTest : public ::testing::Test {
 public:
    MessageCodecTest() {}
    ~MessageCodecTest() {}
};

TEST_F(MessageCodecTest, Codec) {
    Message msg;
    msg.offset = 10;
    std::string val("abcd");
    msg.value = ::openmldb::base::Slice(val);

    ::openmldb::base::Slice data;
    ASSERT_TRUE(::openmldb::codec::MessageCodec::Encode(msg, &data));

    Message decode;
    ASSERT_TRUE(::openmldb::codec::MessageCodec::Decode(data, &decode));
    ASSERT_EQ(decode.version, 1);
    ASSERT_EQ(decode.offset, 10);
    ASSERT_STREQ(decode.value.ToString().c_str(), val.c_str());
}

TEST_F(MessageCodecTest, IoBufEncode) {
    std::string val("abcd");
    ::butil::IOBuf buf;
    buf.append(val);

    ::openmldb::base::Slice data;
    ASSERT_TRUE(::openmldb::codec::MessageCodec::Encode(100, buf, &data));

    Message decode;
    ASSERT_TRUE(::openmldb::codec::MessageCodec::Decode(data, &decode));
    ASSERT_EQ(decode.version, 1);
    ASSERT_EQ(decode.offset, 100);
    ASSERT_STREQ(decode.value.ToString().c_str(), val.c_str());
}

}  // namespace codec
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
