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

#include "cmd/split.h"

#include <memory>
#include <vector>

#include "vm/engine.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"

namespace openmldb {
namespace cmd {

class SplitTest : public ::testing::Test {
 public:
    SplitTest() {}
    ~SplitTest() {}
};

TEST_F(SplitTest, SplitLine) {
    std::vector<std::string> cols;
    openmldb::cmd::SplitLineWithDelimiterForStrings(" --- --- ---", "---", &cols, '\0');
    ASSERT_EQ(cols.size(), 4);
    for (auto& line : cols) {
        ASSERT_EQ(strcmp(line.c_str(), ""), 0);
    }
    cols.clear();

    openmldb::cmd::SplitLineWithDelimiterForStrings("- - ---ab---c b---", "---", &cols, '\0');
    ASSERT_EQ(cols.size(), 4);
    ASSERT_EQ(strcmp(cols[0].c_str(), "- -"), 0);
    ASSERT_EQ(strcmp(cols[1].c_str(), "ab"), 0);
    ASSERT_EQ(strcmp(cols[2].c_str(), "c b"), 0);
    ASSERT_EQ(strcmp(cols[3].c_str(), ""), 0);
    cols.clear();

    openmldb::cmd::SplitLineWithDelimiterForStrings("\" + + \"---+ +---c b", "---", &cols, '"');
    ASSERT_EQ(cols.size(), 3);
    ASSERT_EQ(strcmp(cols[0].c_str(), " + + "), 0);
    ASSERT_EQ(strcmp(cols[1].c_str(), "+ +"), 0);
    ASSERT_EQ(strcmp(cols[2].c_str(), "c b"), 0);
    cols.clear();

    openmldb::cmd::SplitLineWithDelimiterForStrings("\" + + \"---+ +---c b", "---", &cols, '\0');
    ASSERT_EQ(cols.size(), 3);
    ASSERT_EQ(strcmp(cols[0].c_str(), "\" + + \""), 0);
    ASSERT_EQ(strcmp(cols[1].c_str(), "+ +"), 0);
    ASSERT_EQ(strcmp(cols[2].c_str(), "c b"), 0);
    cols.clear();

    openmldb::cmd::SplitLineWithDelimiterForStrings(" _ --- ab --- cd", "---", &cols, '\0');
    ASSERT_EQ(cols.size(), 3);
    ASSERT_EQ(strcmp(cols[0].c_str(), "_"), 0);
    ASSERT_EQ(strcmp(cols[1].c_str(), "ab"), 0);
    ASSERT_EQ(strcmp(cols[2].c_str(), "cd"), 0);
    cols.clear();

    openmldb::cmd::SplitLineWithDelimiterForStrings("ab cd ef", " ", &cols, '\0');
    ASSERT_EQ(cols.size(), 3);
    ASSERT_EQ(strcmp(cols[0].c_str(), "ab"), 0);
    ASSERT_EQ(strcmp(cols[1].c_str(), "cd"), 0);
    ASSERT_EQ(strcmp(cols[2].c_str(), "ef"), 0);
    cols.clear();

    openmldb::cmd::SplitLineWithDelimiterForStrings("ab  cd  ef", " ", &cols, '\0');
    ASSERT_EQ(cols.size(), 5);
    ASSERT_EQ(strcmp(cols[0].c_str(), "ab"), 0);
    ASSERT_EQ(strcmp(cols[1].c_str(), ""), 0);
    ASSERT_EQ(strcmp(cols[2].c_str(), "cd"), 0);
    ASSERT_EQ(strcmp(cols[3].c_str(), ""), 0);
    ASSERT_EQ(strcmp(cols[4].c_str(), "ef"), 0);
    cols.clear();

    openmldb::cmd::SplitLineWithDelimiterForStrings("\"ab \" cd  ef", " ", &cols, '"');
    ASSERT_EQ(cols.size(), 4);
    ASSERT_EQ(strcmp(cols[0].c_str(), "ab "), 0);
    ASSERT_EQ(strcmp(cols[1].c_str(), "cd"), 0);
    ASSERT_EQ(strcmp(cols[2].c_str(), ""), 0);
    ASSERT_EQ(strcmp(cols[3].c_str(), "ef"), 0);
    cols.clear();
}

}  // namespace cmd
}  // namespace openmldb

int main(int argc, char** argv) {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    int ok = RUN_ALL_TESTS();
    return ok;
}
