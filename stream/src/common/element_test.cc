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

#include <gtest/gtest.h>

#include "common/column_element.h"

namespace streaming {
namespace interval_join {

class ElementTest : public ::testing::Test {};

TEST_F(ElementTest, ElementSmokeTest) {
    Element ele("key", "value", 1);
    EXPECT_EQ("key", ele.key());
    EXPECT_EQ("value", ele.value());
    EXPECT_EQ(1, ele.ts());
}

TEST_F(ElementTest, ColumnElementSmokeTest) {
    std::string key = "key";
    std::string val = "val1,val2,val3";
    ColumnElement ele(key, val, 1);
    EXPECT_EQ(key, ele.key());
    EXPECT_EQ(val, ele.value());
    EXPECT_EQ(1, ele.ts());
    EXPECT_EQ(3, ele.GetColumnSize());
    EXPECT_EQ("val1", *ele.GetColumn(0));
    EXPECT_EQ("val2", *ele.GetColumn(1));
    EXPECT_EQ("val3", *ele.GetColumn(2));
}

TEST_F(ElementTest, ElementTypeTest) {
    std::string key = "key";
    std::string val = "val1,val2,val3";
    ColumnElement ele(key, val, 1);
    EXPECT_EQ(ElementType::kUnknown, ele.type());
    EXPECT_EQ(key, ele.key());
    EXPECT_EQ(val, ele.value());
    ele.set_type(ElementType::kProbe);
    EXPECT_EQ(ElementType::kProbe, ele.type());
}


TEST_F(ElementTest, ElementRvalueTest) {
    std::string key = "key";
    std::string val = "val1,val2,val3";
    Element ele(key, val, 1);
    EXPECT_EQ(key, ele.key());
    EXPECT_EQ(val, ele.value());
    EXPECT_EQ(1, ele.ts());
    Element ele2(ele);
    EXPECT_EQ(ele.key(), ele2.key());
    EXPECT_EQ(ele.value(), ele2.value());
    EXPECT_EQ(ele.ts(), ele2.ts());

    Element ele3(std::move(ele));
    EXPECT_EQ(key, ele3.key());
    EXPECT_EQ(val, ele3.value());
    EXPECT_EQ(1, ele3.ts());
    // after rvalue constructor, the `ele` will be moved
    EXPECT_EQ("", ele.key());
    EXPECT_EQ("", ele.value());
    EXPECT_EQ(1, ele.ts());

    auto ele4 = ele3.Clone();
    EXPECT_EQ(key, ele4->key());
    EXPECT_EQ(val, ele4->value());
    EXPECT_EQ(1, ele4->ts());
    // after rvalue constructor, the `ele` will be moved
    EXPECT_EQ("", ele.key());
    EXPECT_EQ("", ele.value());
    EXPECT_EQ(1, ele.ts());

    auto ele5 = const_cast<const Element*>(ele4.get())->Clone();
    EXPECT_EQ(key, ele5->key());
    EXPECT_EQ(val, ele5->value());
    EXPECT_EQ(1, ele5->ts());
    EXPECT_EQ(key, ele4->key());
    EXPECT_EQ(val, ele4->value());
    EXPECT_EQ(1, ele4->ts());
}

TEST_F(ElementTest, ColumnElementRvalueTest) {
    std::string key = "key";
    std::string val = "val1,val2,val3";
    std::vector<std::string> cols = {"val1", "val2", "val3"};
    ColumnElement ele(key, val, 1);
    ele.set_type(ElementType::kProbe);
    ele.set_sid(1);
    ele.set_pid(1);
    EXPECT_EQ(key, ele.key());
    EXPECT_EQ(val, ele.value());
    EXPECT_EQ(1, ele.ts());
    EXPECT_EQ(ElementType::kProbe, ele.type());
    EXPECT_EQ(1, ele.pid());
    EXPECT_EQ(1, ele.sid());
    EXPECT_EQ(cols, ele.cols());
    ColumnElement ele2(ele);
    EXPECT_EQ(ele.key(), ele2.key());
    EXPECT_EQ(ele.value(), ele2.value());
    EXPECT_EQ(ele.ts(), ele2.ts());
    EXPECT_EQ(ElementType::kProbe, ele2.type());
    EXPECT_EQ(1, ele2.pid());
    EXPECT_EQ(1, ele2.sid());
    EXPECT_EQ(cols, ele2.cols());

    ColumnElement ele3(std::move(ele));
    EXPECT_EQ(key, ele3.key());
    EXPECT_EQ(val, ele3.value());
    EXPECT_EQ(1, ele3.ts());
    EXPECT_EQ(ElementType::kProbe, ele3.type());
    EXPECT_EQ(1, ele3.pid());
    EXPECT_EQ(1, ele3.sid());
    EXPECT_EQ(cols, ele3.cols());
    // after rvalue constructor, the `ele` will be moved
    EXPECT_EQ("", ele.key());
    EXPECT_EQ("", ele.value());
    EXPECT_EQ(1, ele.ts());
    EXPECT_EQ(std::vector<std::string>(), ele.cols());

    auto ele4 = ele3.Clone();
    EXPECT_EQ(key, ele4->key());
    EXPECT_EQ(val, ele4->value());
    EXPECT_EQ(1, ele4->ts());
    EXPECT_EQ(ElementType::kProbe, ele4->type());
    EXPECT_EQ(1, ele4->pid());
    EXPECT_EQ(1, ele4->sid());
    EXPECT_EQ(cols, dynamic_cast<ColumnElement*>(ele4.get())->cols());
    EXPECT_EQ("", ele3.key());
    EXPECT_EQ("", ele3.value());
    EXPECT_EQ(1, ele3.ts());
    EXPECT_EQ(std::vector<std::string>(), ele3.cols());

    auto ele5 = const_cast<const Element*>(ele4.get())->Clone();
    EXPECT_EQ(key, ele5->key());
    EXPECT_EQ(val, ele5->value());
    EXPECT_EQ(1, ele5->ts());
    EXPECT_EQ(ElementType::kProbe, ele5->type());
    EXPECT_EQ(1, ele5->pid());
    EXPECT_EQ(1, ele5->sid());
    EXPECT_EQ(cols, dynamic_cast<ColumnElement*>(ele5.get())->cols());
    EXPECT_EQ(key, ele4->key());
    EXPECT_EQ(val, ele4->value());
    EXPECT_EQ(1, ele4->ts());
    EXPECT_EQ(cols, dynamic_cast<ColumnElement*>(ele4.get())->cols());
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
