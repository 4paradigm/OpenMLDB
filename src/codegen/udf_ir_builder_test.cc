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

TEST_F(UDFIRBuilderTest, year_udf_test) {
    base::Status status;
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("udf_test", *ctx);
    UDFIRBuilder udf_ir_builder;
    ASSERT_TRUE(UDFIRBuilder::BuildTimeUDF(m.get(), status));
    m->print(::llvm::errs(), NULL, true, true);

    auto J = ExitOnErr(LLJITBuilder().create());
    auto &jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));

    // Year
    {
        auto fn = ExitOnErr(J->lookup("year"));
        int32_t (*year)(codec::Date *) =
            (int32_t(*)(codec::Date *))fn.getAddress();
        codec::Date d1(2020, 05, 27);
        ASSERT_EQ(2020, year(&d1));
    }

    // Month
    {
        auto fn = ExitOnErr(J->lookup("month"));
        int32_t (*month)(codec::Date *) =
            (int32_t(*)(codec::Date *))fn.getAddress();
        codec::Date d1(2020, 05, 27);
        ASSERT_EQ(05, month(&d1));
    }

    // Day
    {
        auto fn = ExitOnErr(J->lookup("day"));
        int32_t (*day)(codec::Date *) =
            (int32_t(*)(codec::Date *))fn.getAddress();
        codec::Date d1(2020, 05, 27);
        ASSERT_EQ(27, day(&d1));
    }
}

TEST_F(UDFIRBuilderTest, month_udf_test) {
    base::Status status;
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("udf_test", *ctx);
    UDFIRBuilder udf_ir_builder;
    ASSERT_TRUE(UDFIRBuilder::BuildTimeUDF(m.get(), status));
    m->print(::llvm::errs(), NULL, true, true);

    auto J = ExitOnErr(LLJITBuilder().create());
    auto &jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));

    // Month
    {
        auto fn = ExitOnErr(J->lookup("month"));
        int32_t (*month)(codec::Date *) =
            (int32_t(*)(codec::Date *))fn.getAddress();
        codec::Date d1(2020, 05, 27);
        ASSERT_EQ(05, month(&d1));
    }
}

TEST_F(UDFIRBuilderTest, day_udf_test) {
    base::Status status;
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("udf_test", *ctx);
    UDFIRBuilder udf_ir_builder;
    ASSERT_TRUE(UDFIRBuilder::BuildTimeUDF(m.get(), status));
    m->print(::llvm::errs(), NULL, true, true);

    auto J = ExitOnErr(LLJITBuilder().create());
    auto &jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    // Day
    {
        auto fn = ExitOnErr(J->lookup("day"));
        int32_t (*day)(codec::Date *) =
            (int32_t(*)(codec::Date *))fn.getAddress();
        codec::Date d1(2020, 05, 27);
        ASSERT_EQ(27, day(&d1));
    }
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
