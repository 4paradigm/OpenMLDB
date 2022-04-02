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

#include "codegen/predicate_expr_ir_builder.h"
#include <memory>
#include <utility>
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/ir_base_builder_test.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"
#include "udf/default_udf_library.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

using hybridse::udf::Nullable;

ExitOnError ExitOnErr;
namespace hybridse {
namespace codegen {
using openmldb::base::StringRef;
using openmldb::base::Date;
using openmldb::base::Timestamp;
class PredicateIRBuilderTest : public ::testing::Test {
 public:
    PredicateIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~PredicateIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <typename LHS, typename Ret>
void UnaryPredicateExprCheck(LHS left_val, Ret expect,
                             hybridse::node::FnOperator op) {
    auto compiled_func = BuildExprFunction<Ret, LHS>(
        [op](node::NodeManager *nm, node::ExprNode *left) {
            return nm->MakeUnaryExprNode(left, op);
        });
    Ret result = compiled_func(left_val);
    ASSERT_EQ(expect, result);
}

template <typename LHS, typename RHS, typename Ret>
void BinaryPredicateExprCheck(LHS left_val, RHS right_val, Ret expect,
                              hybridse::node::FnOperator op) {
    auto compiled_func = BuildExprFunction<Ret, LHS, RHS>(
        [op](node::NodeManager *nm, node::ExprNode *left,
             node::ExprNode *right) {
            return nm->MakeBinaryExprNode(left, right, op);
        });
    ASSERT_TRUE(compiled_func.valid());
    Ret result = compiled_func(left_val, right_val);
    ASSERT_EQ(expect, result);
}

void PredicateNullCheck(node::FnOperator op) {
    BinaryPredicateExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int16_t>, Nullable<int32_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int16_t>, Nullable<int64_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int16_t>, Nullable<float>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int16_t>, Nullable<double>,
                             Nullable<bool>>(1, nullptr, nullptr, op);

    BinaryPredicateExprCheck<Nullable<int32_t>, Nullable<int16_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int32_t>, Nullable<int64_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int32_t>, Nullable<float>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int32_t>, Nullable<double>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int64_t>, Nullable<int16_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int64_t>, Nullable<int32_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int64_t>, Nullable<float>,
                             Nullable<bool>>(1, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<int64_t>, Nullable<double>,
                             Nullable<bool>>(1, nullptr, nullptr, op);

    BinaryPredicateExprCheck<Nullable<float>, Nullable<int16_t>,
                             Nullable<bool>>(1.0f, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<float>, Nullable<int32_t>,
                             Nullable<bool>>(nullptr, 1.0f, nullptr, op);
    BinaryPredicateExprCheck<Nullable<float>, Nullable<int64_t>,
                             Nullable<bool>>(1.0f, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<float>, Nullable<float>, Nullable<bool>>(
        nullptr, 1.0f, nullptr, op);
    BinaryPredicateExprCheck<Nullable<float>, Nullable<double>, Nullable<bool>>(
        nullptr, 1.0f, nullptr, op);

    BinaryPredicateExprCheck<Nullable<double>, Nullable<int16_t>,
                             Nullable<bool>>(1.0, nullptr, nullptr, op);

    BinaryPredicateExprCheck<Nullable<double>, Nullable<int32_t>,
                             Nullable<bool>>(1.0, nullptr, nullptr, op);

    BinaryPredicateExprCheck<Nullable<double>, Nullable<int64_t>,
                             Nullable<bool>>(1.0, nullptr, nullptr, op);

    BinaryPredicateExprCheck<Nullable<double>, Nullable<float>, Nullable<bool>>(
        1.0, nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<double>, Nullable<double>,
                             Nullable<bool>>(1.0, nullptr, nullptr, op);

    BinaryPredicateExprCheck<Nullable<Timestamp>,
                             Nullable<Timestamp>, Nullable<bool>>(
        Timestamp(1590115420000L), nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<Timestamp>,
                             Nullable<Timestamp>, Nullable<bool>>(
        nullptr, Timestamp(1590115420000L), nullptr, op);

    BinaryPredicateExprCheck<Nullable<Timestamp>,
                             Nullable<Timestamp>, Nullable<bool>>(
        Timestamp(1590115420000L), nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<Date>, Nullable<Date>,
                             Nullable<bool>>(nullptr, Date(2020, 05, 20),
                                             nullptr, op);
    BinaryPredicateExprCheck<Nullable<Date>, Nullable<Date>,
                             Nullable<bool>>(Date(2020, 05, 20), nullptr,
                                             nullptr, op);

    BinaryPredicateExprCheck<Nullable<StringRef>,
                             Nullable<StringRef>, Nullable<bool>>(
        StringRef("abc"), nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<StringRef>,
                             Nullable<StringRef>, Nullable<bool>>(
        nullptr, StringRef("abc"), nullptr, op);
}

TEST_F(PredicateIRBuilderTest, TestEqExprTrue) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(1, 1, true,
                                                     ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(1, 1, true,
                                                     ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(1, 1, true,
                                                     ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<float, float, bool>(

        1.0f, 1.0f, true, ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<double, double, bool>(

        1.0, 1.0, true, ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, float, bool>(1, 1.0f, true,
                                                   ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, double, bool>(1, 1.0, true,
                                                    ::hybridse::node::kFnOpEq);
}

TEST_F(PredicateIRBuilderTest, TestTimestampCompare) {
    Timestamp t1(1590115420000L);
    Timestamp t2(1590115420000L);
    Timestamp t3(1590115430000L);
    Timestamp t4(1590115410000L);
    BinaryPredicateExprCheck<Timestamp, Timestamp, bool>(

        t1, t2, true, ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<Timestamp, Timestamp, bool>(

        t1, t3, true, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<Timestamp, Timestamp, bool>(

        t1, t3, true, ::hybridse::node::kFnOpLe);

    BinaryPredicateExprCheck<Timestamp, Timestamp, bool>(

        t1, t3, true, ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<Timestamp, Timestamp, bool>(

        t1, t4, true, ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<Timestamp, Timestamp, bool>(

        t1, t4, true, ::hybridse::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, TestDateCompare) {
    Date d1(2020, 05, 27);
    Date d2(2020, 05, 27);
    Date d3(2020, 05, 28);
    Date d4(2020, 05, 26);
    BinaryPredicateExprCheck<Date, Date, bool>(
        d1, d2, true, ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<Date, Date, bool>(
        d1, d3, true, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<Date, Date, bool>(
        d1, d3, true, ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<Date, Date, bool>(
        d1, d3, true, ::hybridse::node::kFnOpLe);

    BinaryPredicateExprCheck<Date, Date, bool>(
        d1, d2, true, ::hybridse::node::kFnOpLe);

    BinaryPredicateExprCheck<Date, Date, bool>(
        d1, d2, true, ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<Date, Date, bool>(
        d1, d4, true, ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<Date, Date, bool>(
        d1, d4, true, ::hybridse::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, TestStringStringCompare) {
    StringRef d1("text");
    StringRef d2("text");
    StringRef d3("text1");
    StringRef d4("");
    StringRef d5("text2");
    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d1, d2, true, ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d1, d3, true, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d1, d3, true, ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d3, d5, true, ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d1, d3, true, ::hybridse::node::kFnOpLe);

    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d1, d2, true, ::hybridse::node::kFnOpLe);

    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d1, d2, true, ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d1, d4, true, ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<StringRef, StringRef, bool>(

        d1, d4, true, ::hybridse::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, TestStringAnytypeCompare0) {
    BinaryPredicateExprCheck<StringRef, int32_t, bool>(
        StringRef("123"), 123, true, ::hybridse::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestStringAnytypeCompare1) {
    BinaryPredicateExprCheck<StringRef, int64_t, bool>(
        StringRef("123"), static_cast<int64_t>(123), true,
        ::hybridse::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestStringAnytypeCompare2) {
    BinaryPredicateExprCheck<StringRef, double, bool>(
        StringRef("123"), 123.0, true, ::hybridse::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestStringAnytypeCompare3) {
    BinaryPredicateExprCheck<StringRef, float, bool>(
        StringRef("123"), 123.0f, true, ::hybridse::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestStringAnytypeCompare4) {
    BinaryPredicateExprCheck<StringRef, Date, bool>(
        StringRef("2020-05-30"), Date(2020, 05, 30), true,
        ::hybridse::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestStringAnytypeCompare5) {
    BinaryPredicateExprCheck<StringRef, Timestamp, bool>(
        StringRef("2020-05-22 10:43:40"),
        Timestamp(1590115420000L), true, ::hybridse::node::kFnOpEq);
}

TEST_F(PredicateIRBuilderTest, TestEqExprFalse) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(1, 2, false,
                                                     ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(1, 2, false,
                                                     ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(1, 2, false,
                                                     ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<float, float, bool>(

        1.0f, 1.1f, false, ::hybridse::node::kFnOpEq);

    BinaryPredicateExprCheck<double, double, bool>(

        1.0, 1.1, false, ::hybridse::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestEqNull0) {
    BinaryPredicateExprCheck<Nullable<StringRef>,
                             Nullable<StringRef>, Nullable<bool>>(
        StringRef("abc"), nullptr, nullptr, node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestEqNull1) {
    BinaryPredicateExprCheck<Nullable<StringRef>,
                             Nullable<StringRef>, Nullable<bool>>(
        nullptr, StringRef("abc"), nullptr, node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestEqNull) {
    PredicateNullCheck(node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, TestNeqNull) {
    PredicateNullCheck(node::kFnOpNeq);
}
TEST_F(PredicateIRBuilderTest, TestLqNull) {
    PredicateNullCheck(node::kFnOpLe);
}
TEST_F(PredicateIRBuilderTest, TestLtNull) {
    PredicateNullCheck(node::kFnOpLt);
}
TEST_F(PredicateIRBuilderTest, TestGtNull) {
    PredicateNullCheck(node::kFnOpGt);
}
TEST_F(PredicateIRBuilderTest, TestGeNull) {
    PredicateNullCheck(node::kFnOpGe);
}
TEST_F(PredicateIRBuilderTest, TestNeqExprTrue) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        1, 2, true, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        1, 2, true, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        1, 2, true, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<float, float, bool>(

        1.0f, 1.1f, true, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<double, double, bool>(

        1.0, 1.1, true, ::hybridse::node::kFnOpNeq);
}

TEST_F(PredicateIRBuilderTest, TestNeqExprFalse) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        1, 1, false, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        1, 1, false, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        1, 1, false, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<float, float, bool>(

        1.0f, 1.0f, false, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<double, double, bool>(

        1.0, 1.0, false, ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, float, bool>(1, 1.0f, false,
                                                   ::hybridse::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, double, bool>(1, 1.0, false,
                                                    ::hybridse::node::kFnOpNeq);
}

TEST_F(PredicateIRBuilderTest, TestGtExprTrue) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 1, true,
                                                     ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 1, true,
                                                     ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 1, true,
                                                     ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.0f, true, ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.0, true, ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, float, bool>(2, 1.9f, true,
                                                   ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, double, bool>(2, 1.9, true,
                                                    ::hybridse::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, TestGtExprFalse) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 2, false,
                                                     ::hybridse::node::kFnOpGt);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 3, false,
                                                     ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 2, false,
                                                     ::hybridse::node::kFnOpGt);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 3, false,
                                                     ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 2, false,
                                                     ::hybridse::node::kFnOpGt);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 3, false,
                                                     ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.2f, false, ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.2, false, ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.1f, false,
                                                   ::hybridse::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, double, bool>(2, 2.1, false,
                                                    ::hybridse::node::kFnOpGt);
}
TEST_F(PredicateIRBuilderTest, TestGeExprTrue) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 1, true,
                                                     ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 2, true,
                                                     ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 1, true,
                                                     ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 2, true,
                                                     ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 1, true,
                                                     ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 2, true,
                                                     ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.0f, true, ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.1f, true, ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.0, true, ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.1, true, ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, float, bool>(2, 1.9f, true,
                                                   ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.0f, true,
                                                   ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, double, bool>(2, 1.9, true,
                                                    ::hybridse::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, double, bool>(2, 2.0, true,
                                                    ::hybridse::node::kFnOpGe);
}

TEST_F(PredicateIRBuilderTest, TestGeExprFalse) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 3, false,
                                                     ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 3, false,
                                                     ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 3, false,
                                                     ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.2f, false, ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.2, false, ::hybridse::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.1f, false,
                                                   ::hybridse::node::kFnOpGe);
}

TEST_F(PredicateIRBuilderTest, TestLtExprTrue) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 3, true,
                                                     ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 3, true,
                                                     ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 3, true,
                                                     ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.2f, true, ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.2, true, ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.1f, true,
                                                   ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<Nullable<int32_t>, Nullable<float>,
                             Nullable<bool>>(2, nullptr, nullptr,
                                             ::hybridse::node::kFnOpLt);
}

TEST_F(PredicateIRBuilderTest, TestLtExprFalse) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 1, false,
                                                     ::hybridse::node::kFnOpLt);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 2, false,
                                                     ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 1, false,
                                                     ::hybridse::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 2, false,
                                                     ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 1, false,
                                                     ::hybridse::node::kFnOpLt);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 2, false,
                                                     ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.0f, false, ::hybridse::node::kFnOpLt);
    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.1f, false, ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.0, false, ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.1, false, ::hybridse::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, float, bool>(2, 1.9f, false,
                                                   ::hybridse::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.0f, false,
                                                   ::hybridse::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, double, bool>(2, 1.9, false,
                                                    ::hybridse::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, double, bool>(2, 2.0, false,
                                                    ::hybridse::node::kFnOpLt);
}

TEST_F(PredicateIRBuilderTest, TestAndExpr) {
    auto AndExprCheck = [](Nullable<bool> left, Nullable<bool> right,
                           Nullable<bool> expect) {
        BinaryPredicateExprCheck(left, right, expect,
                                 ::hybridse::node::kFnOpAnd);
    };
    AndExprCheck(true, true, true);
    AndExprCheck(true, false, false);
    AndExprCheck(true, nullptr, nullptr);
    AndExprCheck(false, true, false);
    AndExprCheck(false, false, false);
    AndExprCheck(false, nullptr, false);
    AndExprCheck(nullptr, true, nullptr);
    AndExprCheck(nullptr, false, false);
    AndExprCheck(nullptr, nullptr, nullptr);
}

TEST_F(PredicateIRBuilderTest, TestOrExpr) {
    auto OrExprCheck = [](Nullable<bool> left, Nullable<bool> right,
                          Nullable<bool> expect) {
        BinaryPredicateExprCheck(left, right, expect,
                                 ::hybridse::node::kFnOpOr);
    };
    OrExprCheck(true, true, true);
    OrExprCheck(true, false, true);
    OrExprCheck(true, nullptr, true);
    OrExprCheck(false, true, true);
    OrExprCheck(false, false, false);
    OrExprCheck(false, nullptr, nullptr);
    OrExprCheck(nullptr, true, true);
    OrExprCheck(nullptr, false, nullptr);
    OrExprCheck(nullptr, nullptr, nullptr);
}

TEST_F(PredicateIRBuilderTest, TestXorExpr) {
    auto XorExprCheck = [](Nullable<bool> left, Nullable<bool> right,
                           Nullable<bool> expect) {
        BinaryPredicateExprCheck(left, right, expect,
                                 ::hybridse::node::kFnOpXor);
    };
    XorExprCheck(true, true, false);
    XorExprCheck(true, false, true);
    XorExprCheck(true, nullptr, nullptr);
    XorExprCheck(false, true, true);
    XorExprCheck(false, false, false);
    XorExprCheck(false, nullptr, nullptr);
    XorExprCheck(nullptr, true, nullptr);
    XorExprCheck(nullptr, false, nullptr);
    XorExprCheck(nullptr, nullptr, nullptr);
}

TEST_F(PredicateIRBuilderTest, TestNotExprFalse) {
    UnaryPredicateExprCheck<bool, bool>(true, false,
                                        ::hybridse::node::kFnOpNot);

    UnaryPredicateExprCheck<int32_t, bool>(1, false,
                                           ::hybridse::node::kFnOpNot);
    UnaryPredicateExprCheck<float, bool>(1.0, false,
                                         ::hybridse::node::kFnOpNot);

    UnaryPredicateExprCheck<double, bool>(1.0, false,
                                          ::hybridse::node::kFnOpNot);

    UnaryPredicateExprCheck<bool, bool>(false, true,
                                        ::hybridse::node::kFnOpNot);

    UnaryPredicateExprCheck<int32_t, bool>(0, true, ::hybridse::node::kFnOpNot);
    UnaryPredicateExprCheck<float, bool>(0, true, ::hybridse::node::kFnOpNot);

    UnaryPredicateExprCheck<double, bool>(0, true, ::hybridse::node::kFnOpNot);

    UnaryPredicateExprCheck<Nullable<bool>, Nullable<bool>>(
        nullptr, nullptr, ::hybridse::node::kFnOpNot);
}

}  // namespace codegen
}  // namespace hybridse
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
