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
#include <memory>
#include <string>
#include <utility>
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
#include "node/node_manager.h"
#include "parser/parser.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

class FnIRBuilderTest : public ::testing::Test {
 public:
    FnIRBuilderTest() {
        manager_ = new node::NodeManager();
        parser_ = new parser::FeSQLParser();
    }
    ~FnIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
    parser::FeSQLParser *parser_;
};

TEST_F(FnIRBuilderTest, test_add_int32) {
    const std::string test =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend";
    std::cout << test << std::endl;
    ::fesql::node::NodePointVector trees;
    ::fesql::base::Status status;
    int ret = parser_->parse(test, trees, manager_, status);
    ASSERT_EQ(0, ret);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("custom_fn", *ctx);
    FnIRBuilder fn_ir_builder(m.get());
    std::cout << *trees[0] << std::endl;
    bool ok =
        fn_ir_builder.Build((node::FnNodeFnDef *)trees[0], status);
    ASSERT_TRUE(ok);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto test_jit = ExitOnErr(J->lookup("test"));
    int32_t (*test_fn)(int32_t, int32_t) =
        (int32_t(*)(int32_t, int32_t))test_jit.getAddress();
    int32_t test_ret = test_fn(1, 2);
    ASSERT_EQ(test_ret, 4);
}

TEST_F(FnIRBuilderTest, test_bracket_int32) {
    const std::string test =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a*(b+1)\n    return c\nend";
    node::NodePointVector trees;
    node::PlanNodeList plan_trees;
    base::Status status;
    int ret = parser_->parse(test, trees, manager_, status);
    ASSERT_EQ(0, ret);
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("custom_fn", *ctx);
    FnIRBuilder fn_ir_builder(m.get());
    bool ok = fn_ir_builder.Build(dynamic_cast<node::FnNodeFnDef *>(trees[0]),
                                  status);
    ASSERT_TRUE(ok);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto test_jit = ExitOnErr(J->lookup("test"));
    int32_t (*test_fn)(int32_t, int32_t) =
        (int32_t(*)(int32_t, int32_t))test_jit.getAddress();
    int32_t test_ret = test_fn(1, 2);
    ASSERT_EQ(test_ret, 4);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
