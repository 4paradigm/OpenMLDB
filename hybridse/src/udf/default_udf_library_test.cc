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

#include "udf/default_udf_library.h"
#include <gtest/gtest.h>

namespace hybridse {
namespace udf {

class DefaultUdfLibraryTest : public ::testing::Test {
};

TEST_F(DefaultUdfLibraryTest, TestCheckIsUDAFByNameAndArgsSize) {
    const udf::UdfLibrary* library = udf::DefaultUdfLibrary::get();
    // sum(arg1) is an udaf
    ASSERT_TRUE(library->IsUdaf("sum", 1));
    // sum(arg1, arg2) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("sum", 2));
    // sum2(arg1) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("sum2", 1));

    // max(arg1) is an udaf
    ASSERT_TRUE(library->IsUdaf("max", 1));
    // min(arg1) is an udaf
    ASSERT_TRUE(library->IsUdaf("min", 1));
    // count(arg1) is an udaf
    ASSERT_TRUE(library->IsUdaf("count", 1));
    // avg(arg1) is an udaf
    ASSERT_TRUE(library->IsUdaf("avg", 1));

    // hour(arg1) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("hour", 1));
    // minute(arg1) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("minute", 1));
    // second(arg1) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("second", 1));
    // sin(arg1) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("sin", 1));
}


TEST_F(DefaultUdfLibraryTest, TestCheckIsUdafByName) {
    const udf::UdfLibrary* library = udf::DefaultUdfLibrary::get();
    // sum(...) is an udaf
    ASSERT_TRUE(library->IsUdaf("sum"));
    // sum2(...) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("sum2"));

    // max(...) is an udaf
    ASSERT_TRUE(library->IsUdaf("max"));
    // min(...) is an udaf
    ASSERT_TRUE(library->IsUdaf("min"));
    // count(...) is an udaf
    ASSERT_TRUE(library->IsUdaf("count"));
    // avg(...) is an udaf
    ASSERT_TRUE(library->IsUdaf("avg"));
    // median(...) is an udaf
    ASSERT_TRUE(library->IsUdaf("median"));

    // hour(..) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("hour"));
    // minute(..) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("minute"));
    // second(..) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("second"));
    // sin(..) isn't an udaf
    ASSERT_TRUE(!library->IsUdaf("sin"));
}
}  // namespace udf
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
