/*
 * ir_base_builder_test.cc
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

#include "codegen/expr_ir_builder.h"
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
using namespace llvm;
using namespace llvm::orc;

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

class ExprIRBuilderTest : public ::testing::Test {
 public:
    ExprIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~ExprIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

void GenAddExpr(node::NodeManager *manager, ::fesql::node::ExprNode **expr) {
    // TODO free
    new ::fesql::node::BinaryExpr(::fesql::node::kFnOpAdd);

    ::fesql::node::ExprNode *i32_node =
        (manager->MakeConstNode(1));
    ::fesql::node::ExprNode *id_node =
        (manager->MakeFnIdNode("a"));
    ::fesql::node::ExprNode *bexpr = (manager->MakeBinaryExprNode(
        i32_node, id_node, fesql::node::kFnOpAdd));
    *expr = bexpr;
}

TEST_F(ExprIRBuilderTest, test_add_int32) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("int32_add", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function *load_fn =
        Function::Create(FunctionType::get(Type::getInt32Ty(*ctx),
                                           {Type::getInt32Ty(*ctx)}, false),
                         Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin();
    ScopeVar scope_var;
    scope_var.Enter("fn_base");
    scope_var.AddVar("a", arg0);
    ExprIRBuilder expr_builder(entry_block, &scope_var);
    ::fesql::node::ExprNode *node = NULL;
    GenAddExpr(manager_, &node);
    llvm::Value *output;
    bool ok = expr_builder.Build(node, &output);
    ASSERT_TRUE(ok);
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    int32_t (*decode)(int32_t) = (int32_t(*)(int32_t))load_fn_jit.getAddress();
    int32_t ret = decode(1);
    ASSERT_EQ(ret, 2);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
