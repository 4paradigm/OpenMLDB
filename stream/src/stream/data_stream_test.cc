/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <absl/strings/str_cat.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <fstream>

#include "stream/memory_data_stream.h"
#include "stream/simple_data_stream.h"
#include "common/column_element.h"

namespace streaming {
namespace interval_join {

DECLARE_bool(gen_ts_unique);

class DataStreamTest : public ::testing::Test {};

TEST_F(DataStreamTest, SimpleDataStreamTest) {
    SimpleDataStream ds(100);
    int64_t count = 0;
    while (ds.Get()) {
        count++;
    }
    EXPECT_EQ(100, count);
}

TEST_F(DataStreamTest, SimpleDataStreamMaxTsTest) {
    {
        SimpleDataStream ds(100, 1, 300);
        int64_t count = 0;
        while (auto ele = ds.Get()) {
            ASSERT_EQ(count * 3, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }
    {
        SimpleDataStream ds(100);
        int64_t count = 0;
        while (auto ele = ds.Get()) {
            ASSERT_EQ(count, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }
    {
        SimpleDataStream ds(100, 1, 250);
        int64_t count = 0;
        while (auto ele = ds.Get()) {
            ASSERT_EQ(count * 2, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }
}

TEST_F(DataStreamTest, SimpleDataStreamKeyNumTest) {
    FLAGS_gen_ts_unique = true;
    {
        SimpleDataStream ds(100, 50);
        int64_t count = 0;
        while (auto ele = ds.Get()) {
            ASSERT_EQ("key_" + std::to_string(count % 50), ele->key());
            ASSERT_EQ(std::to_string(count), ele->value());
            ASSERT_EQ(count, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }
    {
        SimpleDataStream ds(100, 100);
        int64_t count = 0;
        while (auto ele = ds.Get()) {
            ASSERT_EQ("key_" + std::to_string(count % 100), ele->key());
            ASSERT_EQ(std::to_string(count), ele->value());
            ASSERT_EQ(count, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }
    {
        SimpleDataStream ds(100);
        int64_t count = 0;
        while (auto ele = ds.Get()) {
            ASSERT_EQ("key_0", ele->key());
            ASSERT_EQ(std::to_string(count), ele->value());
            ASSERT_EQ(count, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }

    FLAGS_gen_ts_unique = false;
    {
        int ele_num = 100;
        int key_num = 50;
        SimpleDataStream ds(ele_num, key_num);
        int64_t count = 0;

        while (auto ele = ds.Get()) {
            ASSERT_EQ("key_" + std::to_string(count % 50), ele->key());
            ASSERT_EQ(count < key_num ? "0" : "1", ele->value());
            ASSERT_EQ(count < key_num ? 0 : 1, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }
    {
        SimpleDataStream ds(100, 100);
        int64_t count = 0;
        while (auto ele = ds.Get()) {
            ASSERT_EQ("key_" + std::to_string(count % 100), ele->key());
            ASSERT_EQ("0", ele->value());
            ASSERT_EQ(0, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }
    {
        SimpleDataStream ds(100);
        int64_t count = 0;
        while (auto ele = ds.Get()) {
            ASSERT_EQ("key_0", ele->key());
            ASSERT_EQ(std::to_string(count), ele->value());
            ASSERT_EQ(count, ele->ts());
            count++;
        }
        EXPECT_EQ(100, count);
    }
}

TEST_F(DataStreamTest, SimpleDataStreamTypeTest) {
    {
        SimpleDataStream ds(100);
        EXPECT_EQ(StreamType::kUnknown, ds.type());
        auto ele = ds.Get();
        EXPECT_EQ(ElementType::kUnknown, ele->type());
    }
    {
        SimpleDataStream ds(100, 1, StreamType::kBase);
        EXPECT_EQ(StreamType::kBase, ds.type());
        auto ele = ds.Get();
        EXPECT_EQ(ElementType::kBase, ele->type());
    }
    {
        SimpleDataStream ds(100, 1, StreamType::kProbe);
        EXPECT_EQ(StreamType::kProbe, ds.type());
        auto ele = ds.Get();
        EXPECT_EQ(ElementType::kProbe, ele->type());
    }
    {
        SimpleDataStream ds(100, 1, StreamType::kMix);
        EXPECT_EQ(StreamType::kMix, ds.type());
        auto ele = ds.Get();
        EXPECT_EQ(ElementType::kUnknown, ele->type());
    }
}

TEST_F(DataStreamTest, MemoryDataStreamTypeTest) {
    {
        MemoryDataStream ds;
        EXPECT_EQ(StreamType::kUnknown, ds.type());
    }

    {
        MemoryDataStream ds(StreamType::kBase);
        EXPECT_EQ(StreamType::kBase, ds.type());
    }

    {
        MemoryDataStream ds(StreamType::kProbe);
        EXPECT_EQ(StreamType::kProbe, ds.type());
    }
}

TEST_F(DataStreamTest, MemoryDataStreamSmokeTest) {
    {
        MemoryDataStream ds;
        EXPECT_EQ(StreamType::kUnknown, ds.type());
        int num = 100;
        // load all the elements to the stream
        for (int i = 0; i < num; i++) {
            Element ele(absl::StrCat("key_", std::to_string(i)), absl::StrCat("value_", std::to_string(i)), i);
            ds.Put(&ele);
        }

        EXPECT_EQ(num, ds.size());

        // read and verify
        int count = 0;
        while (auto ele = ds.Get()) {
            auto key = absl::StrCat("key_", std::to_string(count));
            auto value = absl::StrCat("value_", std::to_string(count));
            EXPECT_EQ(key, ele->key());
            EXPECT_EQ(value, ele->value());
            EXPECT_EQ(count, ele->ts());
            count++;
        }
        EXPECT_EQ(num, count);
    }
}

TEST_F(DataStreamTest, MemoryDataStreamRvalueTest) {
    MemoryDataStream ds;
    EXPECT_EQ(StreamType::kUnknown, ds.type());
    std::string key = "key";
    std::string value = "val1,val2,val3";
    std::vector<std::string> cols = {"val1", "val2", "val3"};
    ColumnElement ele(key, value, 0);
    ds.Put(std::move(ele));

    EXPECT_EQ(1, ds.size());

    // read and verify
    int count = 0;
    while (auto ele = ds.Get()) {
        EXPECT_EQ(key, ele->key());
        EXPECT_EQ(value, ele->value());
        EXPECT_EQ(count, ele->ts());
        count++;
    }
    EXPECT_EQ(1, count);

    EXPECT_EQ("", ele.key());
    EXPECT_EQ("", ele.value());
    EXPECT_EQ(0, ele.ts());
    EXPECT_EQ(std::vector<std::string>(), ele.cols());
}

TEST_F(DataStreamTest, MergedMemoryDataStreamSmokeTest) {
    {
        MemoryDataStream ds0(StreamType::kBase);
        EXPECT_EQ(StreamType::kBase, ds0.type());
        MemoryDataStream ds1(StreamType::kProbe);
        EXPECT_EQ(StreamType::kProbe, ds1.type());

        int num = 100;
        // load all the elements to the stream
        for (int i = 0; i < num; i++) {
            Element ele(absl::StrCat("key_", std::to_string(i)), absl::StrCat("value_", std::to_string(i)), i);
            if (i % 2 == 0) {
                ds0.Put(&ele);
            } else {
                ds1.Put(&ele);
            }
        }

        EXPECT_EQ(num / 2, ds0.size());
        EXPECT_EQ(num / 2, ds1.size());

        MergedMemoryDataStream mds({&ds0, &ds1});
        EXPECT_EQ(num, mds.size());

        // read and verify
        int count = 0;
        while (auto ele = mds.Get()) {
            auto key = absl::StrCat("key_", std::to_string(count));
            auto value = absl::StrCat("value_", std::to_string(count));
            EXPECT_EQ(key, ele->key());
            EXPECT_EQ(value, ele->value());
            EXPECT_EQ(count, ele->ts());
            count++;
        }
        EXPECT_EQ(num, count);
    }
}

TEST_F(DataStreamTest, ElementTypeGetterTest) {
    MemoryDataStream base_stream(StreamType::kBase);
    EXPECT_EQ(base_stream.GetElementType(), ElementType::kBase);

    MemoryDataStream probe_stream(StreamType::kProbe);
    EXPECT_EQ(probe_stream.GetElementType(), ElementType::kProbe);

    MemoryDataStream unknown_stream(StreamType::kUnknown);
    EXPECT_EQ(unknown_stream.GetElementType(), ElementType::kUnknown);

    MemoryDataStream mixed_stream(StreamType::kMix);
    EXPECT_EQ(unknown_stream.GetElementType(), ElementType::kUnknown);
}

TEST_F(DataStreamTest, FileReaderTest) {
    MemoryDataStream stream(StreamType::kBase);
    std::string path = "../../data/input.csv";
    std::string key_col = "key";
    std::string ts_col = "ts";
    std::string val_col = "val";
    stream.ReadFromFile(path, key_col, val_col, ts_col);

    // get baseline time
    std::string ts_str = "2000-01-01 00:00:00";
    std::tm tm = {};
    std::stringstream ts_stream(ts_str);
    ts_stream >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    auto baseline = static_cast<int64_t>(mktime(&tm)) * 1000000;

    int count = 0;
    while (auto ele = stream.Get()) {
        EXPECT_EQ(ele->key(), absl::StrCat("key_", std::to_string(count)));
        EXPECT_EQ(ele->value(), std::to_string(count));
        EXPECT_EQ(ele->ts() - baseline, count * 1000001);
        EXPECT_EQ(ele->type(), ElementType::kBase);
        count++;
    }
    EXPECT_EQ(count, 5);
}

TEST_F(DataStreamTest, FileWriterTest) {
    FLAGS_gen_ts_unique = true;
    int ele_num = 10;
    std::string path = "../../data/output.csv";
    SimpleDataStream stream(ele_num, ele_num);
    stream.WriteToFile(path);

    std::ifstream file(path);
    ASSERT_TRUE(file.is_open());
    std::string row;
    ASSERT_TRUE(std::getline(file, row));
    EXPECT_EQ(row, "key,ts,value");

    for (int i = 0; i < ele_num; i++) {
        ASSERT_TRUE(std::getline(file, row));
        EXPECT_EQ(row, absl::StrCat("key_", i, ",", i, ",", i));
    }
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
