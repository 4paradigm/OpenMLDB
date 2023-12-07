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

#include <vector>

#include "gtest/gtest.h"
#include "sdk/options_map_parser.h"

namespace openmldb {
namespace sdk {

class OptionsMapParserTest : public ::testing::Test {
 public:
    OptionsMapParserTest() {}
    ~OptionsMapParserTest() {}
};

// ptr to node_map, so don't use it after node_map destroyed
std::shared_ptr<hybridse::node::OptionsMap> CvtMap(const OptionsMapParser::OptionsMap& node_map) {
    hybridse::node::OptionsMap ptr_map;
    for (auto& pair : node_map) {
        ptr_map.insert(std::make_pair(pair.first, &pair.second));
    }
    return std::make_shared<hybridse::node::OptionsMap>(ptr_map);
}

TEST_F(OptionsMapParserTest, Load) {
    OptionsMapParser::OptionsMap m = {{"delimiter", hybridse::node::ConstNode("\t")},
                                      {"header", hybridse::node::ConstNode(false)}}; // header will be false
    LoadOptionsMapParser parser(CvtMap(m));
    ASSERT_TRUE(parser.IsLocalMode().ok() && !parser.IsLocalMode().value()) << "default should be cluster mode";
    // if not local mode, don't validate

    m.insert({"load_mode", hybridse::node::ConstNode("local")});
    LoadOptionsMapParser parser1(CvtMap(m));
    ASSERT_TRUE(parser1.IsLocalMode().ok() && parser1.IsLocalMode().value());
    auto st = parser1.Validate();
    ASSERT_TRUE(st.ok()) << st.ToString();
    LOG(INFO) << "parser: " << parser1.ToString();
    auto header = parser1.GetAs<bool>("header");
    ASSERT_TRUE(header.ok() && !header.value());

    // default map
    LoadOptionsMapParser parser2({});
    ASSERT_TRUE(parser2.GetAs<std::string>("quote").ok() && parser2.GetAs<std::string>("quote").value() == "\0");
}

TEST_F(OptionsMapParserTest, Write) {
    OptionsMapParser::OptionsMap m = {{"quote", hybridse::node::ConstNode("\"")},
                                      {"header", hybridse::node::ConstNode(false)}};
    WriteOptionsMapParser parser(CvtMap(m));
    ASSERT_FALSE(parser.GetAs<std::string>("load_mode").ok()) << "write options should not have load_mode";
    ASSERT_TRUE(parser.GetAs<bool>("header").ok() && !parser.GetAs<bool>("header").value());
    ASSERT_TRUE(parser.GetAs<std::string>("quote").ok() && parser.GetAs<std::string>("quote").value() == "\"");

    // default map
    WriteOptionsMapParser parser1({});
    ASSERT_TRUE(parser1.GetAs<std::string>("quote").ok() && parser1.GetAs<std::string>("quote").value() == "\0");
}
}  // namespace sdk
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
