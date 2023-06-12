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

#include "sdk/result_set_sql.h"

#include <string>
#include <vector>
#include "codec/schema_codec.h"
#include "gtest/gtest.h"

namespace openmldb::sdk {

using ::openmldb::codec::SchemaCodec;

class ResultSetSQLTest : public ::testing::Test {};

TEST_F(ResultSetSQLTest, ReadableResultSet) {
    ::openmldb::schema::PBSchema schema;
    SchemaCodec::SetColumnDesc(schema.Add(), "col1", ::openmldb::type::kString);
    SchemaCodec::SetColumnDesc(schema.Add(), "col2", ::openmldb::type::kBigInt);
    SchemaCodec::SetColumnDesc(schema.Add(), "col3", ::openmldb::type::kTimestamp);
    SchemaCodec::SetColumnDesc(schema.Add(), "col4", ::openmldb::type::kDate);
    hybridse::sdk::Status status;
    std::vector<std::vector<std::string>> data = {{"colxxx", "12345", "1664252237000", "2022-09-27"}};
    auto rs = ResultSetSQL::MakeResultSet(schema, data, &status);
    ASSERT_TRUE(status.IsOK());
    auto readable_rs = std::make_shared<ReadableResultSetSQL>(rs);
    readable_rs->Next();
    std::string val;
    ASSERT_TRUE(readable_rs->GetAsString(2, val));
    ASSERT_EQ(val, "2022-09-27 12:17:17");
    ASSERT_TRUE(readable_rs->GetAsString(3, val));
    ASSERT_EQ(val, "2022-09-27");
}

}  // namespace openmldb::sdk

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
