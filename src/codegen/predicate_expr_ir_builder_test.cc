/*
 * Copyright (c) 2021 4Paradigm
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

using fesql::udf::Nullable;

ExitOnError ExitOnErr;
namespace fesql {
namespace codegen {
class PredicateIRBuilderTest : public ::testing::Test {
 public:
    PredicateIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~PredicateIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <typename LHS, typename Ret>
void UnaryPredicateExprCheck(LHS left_val, Ret expect,
                             fesql::node::FnOperator op) {
    auto compiled_func = BuildExprFunction<Ret, LHS>(
        [op](node::NodeManager *nm, node::ExprNode *left) {
            return nm->MakeUnaryExprNode(left, op);
        });
    Ret result = compiled_func(left_val);
    ASSERT_EQ(expect, result);
}

template <typename LHS, typename RHS, typename Ret>
void BinaryPredicateExprCheck(LHS left_val, RHS right_val, Ret expect,
                              fesql::node::FnOperator op) {
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

    BinaryPredicateExprCheck<Nullable<codec::Timestamp>,
                             Nullable<codec::Timestamp>, Nullable<bool>>(
        codec::Timestamp(1590115420000L), nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<codec::Timestamp>,
                             Nullable<codec::Timestamp>, Nullable<bool>>(
        nullptr, codec::Timestamp(1590115420000L), nullptr, op);

    BinaryPredicateExprCheck<Nullable<codec::Timestamp>,
                             Nullable<codec::Timestamp>, Nullable<bool>>(
        codec::Timestamp(1590115420000L), nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<codec::Date>, Nullable<codec::Date>,
                             Nullable<bool>>(nullptr, codec::Date(2020, 05, 20),
                                             nullptr, op);
    BinaryPredicateExprCheck<Nullable<codec::Date>, Nullable<codec::Date>,
                             Nullable<bool>>(codec::Date(2020, 05, 20), nullptr,
                                             nullptr, op);

    BinaryPredicateExprCheck<Nullable<codec::StringRef>,
                             Nullable<codec::StringRef>, Nullable<bool>>(
        codec::StringRef("abc"), nullptr, nullptr, op);
    BinaryPredicateExprCheck<Nullable<codec::StringRef>,
                             Nullable<codec::StringRef>, Nullable<bool>>(
        nullptr, codec::StringRef("abc"), nullptr, op);
}

TEST_F(PredicateIRBuilderTest, test_eq_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(1, 1, true,
                                                     ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(1, 1, true,
                                                     ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(1, 1, true,
                                                     ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<float, float, bool>(

        1.0f, 1.0f, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<double, double, bool>(

        1.0, 1.0, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, float, bool>(1, 1.0f, true,
                                                   ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, double, bool>(1, 1.0, true,
                                                    ::fesql::node::kFnOpEq);
}

TEST_F(PredicateIRBuilderTest, test_timestamp_compare) {
    codec::Timestamp t1(1590115420000L);
    codec::Timestamp t2(1590115420000L);
    codec::Timestamp t3(1590115430000L);
    codec::Timestamp t4(1590115410000L);
    BinaryPredicateExprCheck<codec::Timestamp, codec::Timestamp, bool>(

        t1, t2, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<codec::Timestamp, codec::Timestamp, bool>(

        t1, t3, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<codec::Timestamp, codec::Timestamp, bool>(

        t1, t3, true, ::fesql::node::kFnOpLe);

    BinaryPredicateExprCheck<codec::Timestamp, codec::Timestamp, bool>(

        t1, t3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<codec::Timestamp, codec::Timestamp, bool>(

        t1, t4, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<codec::Timestamp, codec::Timestamp, bool>(

        t1, t4, true, ::fesql::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, test_date_compare) {
    codec::Date d1(2020, 05, 27);
    codec::Date d2(2020, 05, 27);
    codec::Date d3(2020, 05, 28);
    codec::Date d4(2020, 05, 26);
    BinaryPredicateExprCheck<codec::Date, codec::Date, bool>(
        d1, d2, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<codec::Date, codec::Date, bool>(
        d1, d3, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<codec::Date, codec::Date, bool>(
        d1, d3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<codec::Date, codec::Date, bool>(
        d1, d3, true, ::fesql::node::kFnOpLe);

    BinaryPredicateExprCheck<codec::Date, codec::Date, bool>(
        d1, d2, true, ::fesql::node::kFnOpLe);

    BinaryPredicateExprCheck<codec::Date, codec::Date, bool>(
        d1, d2, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<codec::Date, codec::Date, bool>(
        d1, d4, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<codec::Date, codec::Date, bool>(
        d1, d4, true, ::fesql::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, test_string_string_compare) {
    codec::StringRef d1("text");
    codec::StringRef d2("text");
    codec::StringRef d3("text1");
    codec::StringRef d4("");
    codec::StringRef d5("text2");
    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d1, d2, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d1, d3, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d1, d3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d3, d5, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d1, d3, true, ::fesql::node::kFnOpLe);

    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d1, d2, true, ::fesql::node::kFnOpLe);

    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d1, d2, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d1, d4, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<codec::StringRef, codec::StringRef, bool>(

        d1, d4, true, ::fesql::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, test_string_anytype_compare_0) {
    BinaryPredicateExprCheck<codec::StringRef, int32_t, bool>(
        codec::StringRef("123"), 123, true, ::fesql::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_string_anytype_compare_1) {
    BinaryPredicateExprCheck<codec::StringRef, int64_t, bool>(
        codec::StringRef("123"), static_cast<int64_t>(123), true,
        ::fesql::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_string_anytype_compare_2) {
    BinaryPredicateExprCheck<codec::StringRef, double, bool>(
        codec::StringRef("123"), 123.0, true, ::fesql::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_string_anytype_compare_3) {
    BinaryPredicateExprCheck<codec::StringRef, float, bool>(
        codec::StringRef("123"), 123.0f, true, ::fesql::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_string_anytype_compare_4) {
    BinaryPredicateExprCheck<codec::StringRef, codec::Date, bool>(
        codec::StringRef("2020-05-30"), codec::Date(2020, 05, 30), true,
        ::fesql::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_string_anytype_compare_5) {
    BinaryPredicateExprCheck<codec::StringRef, codec::Timestamp, bool>(
        codec::StringRef("2020-05-22 10:43:40"),
        codec::Timestamp(1590115420000L), true, ::fesql::node::kFnOpEq);
}

TEST_F(PredicateIRBuilderTest, test_eq_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(1, 2, false,
                                                     ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(1, 2, false,
                                                     ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(1, 2, false,
                                                     ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<float, float, bool>(

        1.0f, 1.1f, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<double, double, bool>(

        1.0, 1.1, false, ::fesql::node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_eq_null_0) {
    BinaryPredicateExprCheck<Nullable<codec::StringRef>,
                             Nullable<codec::StringRef>, Nullable<bool>>(
        codec::StringRef("abc"), nullptr, nullptr, node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_eq_null_1) {
    BinaryPredicateExprCheck<Nullable<codec::StringRef>,
                             Nullable<codec::StringRef>, Nullable<bool>>(
        nullptr, codec::StringRef("abc"), nullptr, node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_eq_null) {
    PredicateNullCheck(node::kFnOpEq);
}
TEST_F(PredicateIRBuilderTest, test_neq_null) {
    PredicateNullCheck(node::kFnOpNeq);
}
TEST_F(PredicateIRBuilderTest, test_lq_null) {
    PredicateNullCheck(node::kFnOpLe);
}
TEST_F(PredicateIRBuilderTest, test_lt_null) {
    PredicateNullCheck(node::kFnOpLt);
}
TEST_F(PredicateIRBuilderTest, test_gt_null) {
    PredicateNullCheck(node::kFnOpGt);
}
TEST_F(PredicateIRBuilderTest, test_ge_null) {
    PredicateNullCheck(node::kFnOpGe);
}
TEST_F(PredicateIRBuilderTest, test_neq_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(1, 2, true,
                                                     ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(1, 2, true,
                                                     ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(1, 2, true,
                                                     ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<float, float, bool>(

        1.0f, 1.1f, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<double, double, bool>(

        1.0, 1.1, true, ::fesql::node::kFnOpNeq);
}

TEST_F(PredicateIRBuilderTest, test_neq_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(1, 1, false,
                                                     ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(1, 1, false,
                                                     ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(1, 1, false,
                                                     ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<float, float, bool>(

        1.0f, 1.0f, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<double, double, bool>(

        1.0, 1.0, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, float, bool>(1, 1.0f, false,
                                                   ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, double, bool>(1, 1.0, false,
                                                    ::fesql::node::kFnOpNeq);
}

TEST_F(PredicateIRBuilderTest, test_gt_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 1, true,
                                                     ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 1, true,
                                                     ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 1, true,
                                                     ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.0f, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.0, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, float, bool>(2, 1.9f, true,
                                                   ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, double, bool>(2, 1.9, true,
                                                    ::fesql::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, test_gt_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 2, false,
                                                     ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 3, false,
                                                     ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 2, false,
                                                     ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 3, false,
                                                     ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 2, false,
                                                     ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 3, false,
                                                     ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.2f, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.2, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.1f, false,
                                                   ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, double, bool>(2, 2.1, false,
                                                    ::fesql::node::kFnOpGt);
}
TEST_F(PredicateIRBuilderTest, test_ge_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 1, true,
                                                     ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 2, true,
                                                     ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 1, true,
                                                     ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 2, true,
                                                     ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 1, true,
                                                     ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 2, true,
                                                     ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.0f, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.1f, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.0, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, float, bool>(2, 1.9f, true,
                                                   ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.0f, true,
                                                   ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, double, bool>(2, 1.9, true,
                                                    ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, double, bool>(2, 2.0, true,
                                                    ::fesql::node::kFnOpGe);
}

TEST_F(PredicateIRBuilderTest, test_ge_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 3, false,
                                                     ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 3, false,
                                                     ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 3, false,
                                                     ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.2f, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.2, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.1f, false,
                                                   ::fesql::node::kFnOpGe);
}

TEST_F(PredicateIRBuilderTest, test_lt_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 3, true,
                                                     ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 3, true,
                                                     ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 3, true,
                                                     ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.2f, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.2, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.1f, true,
                                                   ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<Nullable<int32_t>, Nullable<float>,
                             Nullable<bool>>(2, nullptr, nullptr,
                                             ::fesql::node::kFnOpLt);
}

TEST_F(PredicateIRBuilderTest, test_lt_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 1, false,
                                                     ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(2, 2, false,
                                                     ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 1, false,
                                                     ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(2, 2, false,
                                                     ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 1, false,
                                                     ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(2, 2, false,
                                                     ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.0f, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<float, float, bool>(

        1.1f, 1.1f, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.0, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(

        1.1, 1.1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, float, bool>(2, 1.9f, false,
                                                   ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, float, bool>(2, 2.0f, false,
                                                   ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, double, bool>(2, 1.9, false,
                                                    ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, double, bool>(2, 2.0, false,
                                                    ::fesql::node::kFnOpLt);
}

TEST_F(PredicateIRBuilderTest, test_and_expr) {
    auto AndExprCheck = [](Nullable<bool> left, Nullable<bool> right,
                           Nullable<bool> expect) {
        BinaryPredicateExprCheck(left, right, expect, ::fesql::node::kFnOpAnd);
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

TEST_F(PredicateIRBuilderTest, test_or_expr) {
    auto OrExprCheck = [](Nullable<bool> left, Nullable<bool> right,
                          Nullable<bool> expect) {
        BinaryPredicateExprCheck(left, right, expect, ::fesql::node::kFnOpOr);
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

TEST_F(PredicateIRBuilderTest, test_xor_expr) {
    auto XorExprCheck = [](Nullable<bool> left, Nullable<bool> right,
                           Nullable<bool> expect) {
        BinaryPredicateExprCheck(left, right, expect, ::fesql::node::kFnOpXor);
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

TEST_F(PredicateIRBuilderTest, test_not_expr_false) {
    UnaryPredicateExprCheck<bool, bool>(true, false, ::fesql::node::kFnOpNot);

    UnaryPredicateExprCheck<int32_t, bool>(1, false, ::fesql::node::kFnOpNot);
    UnaryPredicateExprCheck<float, bool>(1.0, false, ::fesql::node::kFnOpNot);

    UnaryPredicateExprCheck<double, bool>(1.0, false, ::fesql::node::kFnOpNot);

    UnaryPredicateExprCheck<bool, bool>(false, true, ::fesql::node::kFnOpNot);

    UnaryPredicateExprCheck<int32_t, bool>(0, true, ::fesql::node::kFnOpNot);
    UnaryPredicateExprCheck<float, bool>(0, true, ::fesql::node::kFnOpNot);

    UnaryPredicateExprCheck<double, bool>(0, true, ::fesql::node::kFnOpNot);

    UnaryPredicateExprCheck<Nullable<bool>, Nullable<bool>>(
        nullptr, nullptr, ::fesql::node::kFnOpNot);
}

}  // namespace codegen
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
