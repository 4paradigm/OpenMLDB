/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_ir_builder_test.cc
 *
 * Author: chenjing
 * Date: 2020/6/17
 *--------------------------------------------------------------------------
 **/
#include "codegen/udf_ir_builder.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "codec/list_iterator_codec.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Utils.h"
#include "node/node_manager.h"
#include "parser/parser.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/sql_compiler.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

class UDFIRBuilderTest : public ::testing::Test {
 public:
    UDFIRBuilderTest() {}
    ~UDFIRBuilderTest() {}
};
template <class T, class... Args>
void CheckExternalUDF(const std::string udf_name, T exp, Args... args) {
    base::Status status;
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("udf_test", *ctx);
    udf::DefaultUDFLibrary lib;
    ASSERT_TRUE(fesql::udf::RegisterUDFToModule(m.get()));
    m->print(::llvm::errs(), NULL, true, true);

    auto J = ExitOnErr(LLJITBuilder().create());
    auto &jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    lib.InitJITSymbols(J.get());
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto fn = ExitOnErr(J->lookup(udf_name));
    T (*udf)(Args...) = (T(*)(Args...))fn.getAddress();
    ASSERT_EQ(exp, udf(args...));
}
// TEST_F(UDFIRBuilderTest, dayofmonth_date_udf_test) {
//    codec::Date date(2020, 05, 22);
//    CheckExternalUDF<int32_t, codec::Date *>("dayofmonth.date", 22, &date);
//}
// TEST_F(UDFIRBuilderTest, month_date_udf_test) {
//    codec::Date date(2020, 05, 22);
//    CheckExternalUDF<int32_t, codec::Date *>("month.date", 5, &date);
//}
// TEST_F(UDFIRBuilderTest, year_date_udf_test) {
//    codec::Date date(2020, 05, 22);
//    CheckExternalUDF<int32_t, codec::Date *>("year.date", 2020, &date);
//}
TEST_F(UDFIRBuilderTest, dayofweek_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckExternalUDF<int32_t, codec::Date *>("dayofweek.date", 6, &date);
}
TEST_F(UDFIRBuilderTest, weekofyear_date_udf_test) {
    {
        codec::Date date(2020, 01, 01);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 1, &date);
    }
    {
        codec::Date date(2020, 01, 02);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 1, &date);
    }
    {
        codec::Date date(2020, 01, 03);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 1, &date);
    }
    {
        codec::Date date(2020, 01, 04);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 1, &date);
    }
    {
        codec::Date date(2020, 01, 05);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 1, &date);
    }
    {
        codec::Date date(2020, 01, 06);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 2, &date);
    }
    {
        codec::Date date(2020, 05, 22);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 21, &date);
    }
    {
        codec::Date date(2020, 05, 23);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 21, &date);
    }
    {
        codec::Date date(2020, 05, 24);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 21, &date);
    }
    {
        codec::Date date(2020, 05, 25);
        CheckExternalUDF<int32_t, codec::Date *>("weekofyear.date", 22, &date);
    }
}

// TEST_F(UDFIRBuilderTest, minute_timestamp_udf_test) {
//    codec::Timestamp time(1590115420000L);
//    CheckExternalUDF<int32_t, codec::Timestamp *>("minute.timestamp", 43,
//                                                  &time);
//}
// TEST_F(UDFIRBuilderTest, second_timestamp_udf_test) {
//    codec::Timestamp time(1590115420000L);
//    CheckExternalUDF<int32_t, codec::Timestamp *>("second.timestamp", 40,
//                                                  &time);
//}
// TEST_F(UDFIRBuilderTest, hour_timestamp_udf_test) {
//    codec::Timestamp time(1590115420000L);
//    CheckExternalUDF<int32_t, codec::Timestamp *>("hour.timestamp", 10,
//    &time);
//}
TEST_F(UDFIRBuilderTest, dayofmonth_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckExternalUDF<int32_t, codec::Timestamp *>("dayofmonth.timestamp", 22,
                                                  &time);
}

TEST_F(UDFIRBuilderTest, dayofweek_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckExternalUDF<int32_t, codec::Timestamp *>("dayofweek.timestamp", 6,
                                                  &time);
}
TEST_F(UDFIRBuilderTest, weekofyear_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckExternalUDF<int32_t, codec::Timestamp *>("weekofyear.timestamp", 21,
                                                  &time);
}

// TEST_F(UDFIRBuilderTest, month_date_udf_test) {
//    codec::Date date(2020, 05, 22);
//    CheckExternalUDF<int32_t, codec::Date *>("month.date", 5, &date);
//}
// TEST_F(UDFIRBuilderTest, year_date_udf_test) {
//    codec::Date date(2020, 05, 22);
//    CheckExternalUDF<int32_t, codec::Date *>("year.date", 2020, &date);
//}

// TEST_F(UDFIRBuilderTest, minute_int64_udf_test) {
//    CheckExternalUDF<int32_t, int64_t>("minute.int64", 43, 1590115420000L);
//}
// TEST_F(UDFIRBuilderTest, second_int64_udf_test) {
//    codec::Timestamp time(1590115420000L);
//    CheckExternalUDF<int32_t, int64_t>("second.int64", 40, 1590115420000L);
//}
// TEST_F(UDFIRBuilderTest, hour_int64_udf_test) {
//    codec::Timestamp time(1590115420000L);
//    CheckExternalUDF<int32_t, int64_t>("hour.int64", 10, 1590115420000L);
//}

TEST_F(UDFIRBuilderTest, dayofmonth_int64_udf_test) {
    CheckExternalUDF<int32_t, int64_t>("dayofmonth.int64", 22, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, month_int64_udf_test) {
    CheckExternalUDF<int32_t, int64_t>("month.int64", 5, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, year_int64_udf_test) {
    CheckExternalUDF<int32_t, int64_t>("year.int64", 2020, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, dayofweek_int64_udf_test) {
    CheckExternalUDF<int32_t, int64_t>("dayofweek.int64", 6, 1590115420000L);
    CheckExternalUDF<int32_t, int64_t>("dayofweek.int64", 7,
                                       1590115420000L + 86400000L);

    // Sunday
    CheckExternalUDF<int32_t, int64_t>("dayofweek.int64", 1,
                                       1590115420000L + 2 * 86400000L);
    CheckExternalUDF<int32_t, int64_t>("dayofweek.int64", 2,
                                       1590115420000L + 3 * 86400000L);
}
TEST_F(UDFIRBuilderTest, weekofyear_int64_udf_test) {
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 21, 1590115420000L);
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 21,
                                       1590115420000L + 86400000L);

    //     Sunday
    CheckExternalUDF<int32_t, int64_t>("dayofmonth.int64", 24,
                                       1590115420000L + 2 * 86400000L);
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 21,
                                       1590115420000L + 2 * 86400000L);
    //     Monday
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 22,
                                       1590115420000L + 3 * 86400000L);
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 22,
                                       1590115420000L + 4 * 86400000L);
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 22,
                                       1590115420000L + 5 * 86400000L);
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 22,
                                       1590115420000L + 6 * 86400000L);
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 22,
                                       1590115420000L + 7 * 86400000L);
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 22,
                                       1590115420000L + 8 * 86400000L);
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 22,
                                       1590115420000L + 9 * 86400000L);

    // Monday
    CheckExternalUDF<int32_t, int64_t>("weekofyear.int64", 23,
                                       1590115420000L + 10 * 86400000L);
}
TEST_F(UDFIRBuilderTest, inc_int32_udf_test) {
    CheckExternalUDF<int32_t, int32_t>("inc.int32", 2021, 2020);
}
TEST_F(UDFIRBuilderTest, distinct_count_udf_test) {
    std::vector<int32_t> vec = {1, 1, 3, 3, 5, 5, 7, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    CheckExternalUDF<int32_t, codec::ListRef<> *>("count.list_int32", 9,
                                                  &list_ref);
    CheckExternalUDF<int32_t, codec::ListRef<> *>("distinct_count.list_int32",
                                                  5, &list_ref);
}
TEST_F(UDFIRBuilderTest, sum_udf_test) {
    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckExternalUDF<int32_t, codec::ListRef<> *>("sum.list_int32",
                                                  1 + 3 + 5 + 7 + 9, &list_ref);
}
TEST_F(UDFIRBuilderTest, min_udf_test) {
    std::vector<int32_t> vec = {10, 8, 6, 4, 2, 1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckExternalUDF<int32_t, codec::ListRef<> *>("min.list_int32", 1,
                                                  &list_ref);
}
TEST_F(UDFIRBuilderTest, max_udf_test) {
    std::vector<int32_t> vec = {10, 8, 6, 4, 2, 1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckExternalUDF<int32_t, codec::ListRef<> *>("max.list_int32", 10,
                                                  &list_ref);
}
TEST_F(UDFIRBuilderTest, max_timestamp_udf_test) {
    std::vector<codec::Timestamp> vec = {
        codec::Timestamp(1590115390000L), codec::Timestamp(1590115410000L),
        codec::Timestamp(1590115420000L), codec::Timestamp(1590115430000L),
        codec::Timestamp(1590115400000L)};
    codec::ArrayListV<codec::Timestamp> list(&vec);
    codec::ListRef<> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    codec::Timestamp max_time;
    CheckExternalUDF<bool, codec::ListRef<> *, codec::Timestamp *>(
        "max.list_timestamp.timestamp", true, &list_ref, &max_time);
    ASSERT_EQ(codec::Timestamp(1590115430000L), max_time);
}
TEST_F(UDFIRBuilderTest, min_timestamp_udf_test) {
    std::vector<codec::Timestamp> vec = {
        codec::Timestamp(1590115390000L), codec::Timestamp(1590115410000L),
        codec::Timestamp(1590115420000L), codec::Timestamp(1590115430000L),
        codec::Timestamp(1590115400000L)};
    codec::ArrayListV<codec::Timestamp> list(&vec);
    codec::ListRef<> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    codec::Timestamp max_time;
    CheckExternalUDF<bool, codec::ListRef<> *, codec::Timestamp *>(
        "min.list_timestamp.timestamp", true, &list_ref, &max_time);
    ASSERT_EQ(codec::Timestamp(1590115390000L), max_time);
}

TEST_F(UDFIRBuilderTest, log_udf_test) {
    CheckExternalUDF<float, float>("log.float", log(2.0f), 2.0f);
    CheckExternalUDF<double, double>("log.double", log(2.0), 2.0);
    CheckExternalUDF<double, int32_t>("log2.int32", log2(65536), 65536);
    CheckExternalUDF<double, double>("log2.double", log2(2.0), 2.0);
    CheckExternalUDF<double, int32_t>("log10.int32", log10(65536), 65536);
    CheckExternalUDF<double, double>("log10.double", log10(2.0), 2.0);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
