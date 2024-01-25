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

#include "sdk/split.h"

#include <memory>
#include <vector>

#include "gflags/gflags.h"
#include "gtest/gtest-param-test.h"
#include "gtest/gtest.h"
#include "vm/engine.h"

namespace openmldb {
namespace sdk {

struct SplitTestCase {
    std::vector<std::string> expect;
    std::string input;
    std::string delimit;
    char enclosed;
};

class SplitTest : public ::testing::TestWithParam<SplitTestCase> {
 public:
    SplitTest() {}
    ~SplitTest() {}
};

static const std::vector<SplitTestCase> cases = {
    { {"abc", ""}, "abc,", ",", '"'},
    { {"abc", ""}, "abc  ,", ",", '"'},
    { { "", "", "", "" }, " --- --- ---", "---", '\0' },
    { { "- -", "ab", "c b", "" }, "- - ---ab---c b---", "---", '\0' },
    { { " + + ", "+ +", "c b" }, "\" + + \"---+ +---c b", "---", '"' },
    { { "\" + + \"", "+ +", "c b" }, "\" + + \"---+ +---c b", "---", '\0' },
    { { "_", "ab", "cd" }, " _ --- ab --- cd", "---", '\0' },
    { { "ab", "cd", "ef" }, "ab cd ef", " ", '\0' },
    { { "ab", "", "cd", "", "ef" }, "ab  cd  ef", " ", '\0' },
    { { "ab ", "cd", "", "ef" }, "\"ab \" cd  ef", " ", '"' },
};

INSTANTIATE_TEST_SUITE_P(SplitLine, SplitTest, testing::ValuesIn(cases));

TEST_P(SplitTest, SplitLineWithDelimiterForStrings) {
    auto& c = GetParam();
    std::vector<std::string> splited;
    SplitLineWithDelimiterForStrings(c.input, c.delimit, &splited, c.enclosed);

    ASSERT_EQ(c.expect.size(), splited.size()) << "splited list size not match";

    for (size_t i = 0; i < c.expect.size(); i++) {
        EXPECT_STREQ(c.expect[i].c_str(), splited[i].c_str());
    }
}

TEST_F(SplitTest, failedCases) {
    // escape 
    std::vector<std::string> splited;
    // "abc\"", quote is ", should be abc\"(char 4), but this method will return abc\, missing the last char
    SplitLineWithDelimiterForStrings("\"abc\\\"\"", ",", &splited, '"');
    ASSERT_EQ(1, splited.size());
    EXPECT_STREQ("abc\\", splited[0].c_str());
}

}  // namespace sdk
}  // namespace openmldb

int main(int argc, char** argv) {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    int ok = RUN_ALL_TESTS();
    return ok;
}
