/*
 * fn_ir_builder_test.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/fn_ir_builder.h"
#include "gtest/gtest.h"
#include "ast/fn_parser.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"


using namespace llvm;
using namespace llvm::orc;

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

class FnIRBuilderTest : public ::testing::Test {

public:
    FnIRBuilderTest() {}
    ~FnIRBuilderTest() {}
};

TEST_F(FnIRBuilderTest, test_add_int32) {
    yyscan_t scan;
    int ret0 = fnlex_init(&scan);
    ASSERT_EQ(0, ret0);
    const char* test ="def test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return d";
    fn_scan_string(test, scan);
    ::fesql::ast::FnNode node;
    int ret = fnparse(scan, &node);
    ASSERT_EQ(0, ret);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("custom_fn", *ctx);
    FnIRBuilder fn_ir_builder(m.get());
    bool ok = fn_ir_builder.Build(&node);
    ASSERT_TRUE(ok);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto test_jit = ExitOnErr(J->lookup("test"));
    int32_t (*test_fn)(int32_t, int32_t) = (int32_t (*)(int32_t, int32_t))test_jit.getAddress();
    int32_t test_ret = test_fn(1, 2);
    ASSERT_EQ(test_ret, 4);
}

} // namespace of codegen
} // namespace of fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}



