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
#include <llvm/Transforms/Utils.h>
#include <storage/window.h>
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
#include "udf/udf.h"

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

template <class R, class V1, class V2>
void CheckResult(std::string test, R res, V1 a, V2 b) {
    node::NodePointVector trees;
    node::PlanNodeList plan_trees;
    base::Status status;
    parser::FeSQLParser parser;
    node::NodeManager manager;
    int ret = parser.parse(test, trees, &manager, status);
    ASSERT_EQ(0, ret);
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("custom_fn", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    FnIRBuilder fn_ir_builder(m.get());
    node::FnNodeFnDef *fn_def = dynamic_cast<node::FnNodeFnDef *>(trees[0]);
    bool ok = fn_ir_builder.Build(fn_def, status);
    ASSERT_TRUE(ok);
    m->print(::llvm::errs(), NULL, true, true);
    LOG(INFO) << "before opt with ins cnt " << m->getInstructionCount();
    ::llvm::legacy::FunctionPassManager fpm(m.get());
    fpm.add(::llvm::createPromoteMemoryToRegisterPass());
    fpm.doInitialization();
    fpm.doInitialization();
    ::llvm::Module::iterator it;
    ::llvm::Module::iterator end = m->end();
    for (it = m->begin(); it != end; ++it) {
        fpm.run(*it);
    }
    LOG(INFO) << "after opt with ins cnt " << m->getInstructionCount();
    m->print(::llvm::errs(), NULL, true, true);
    auto J = ExitOnErr(LLJITBuilder().create());
    auto &jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::storage::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);

    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto test_jit =
        ExitOnErr(J->lookup(fn_def->header_->GetCodegenFunctionName()));
    R (*test_fn)(V1, V2) = (R(*)(V1, V2))test_jit.getAddress();
    ASSERT_EQ(res, test_fn(a, b));
}

void CheckResult(std::string test, int32_t res, int32_t a, int32_t b) {
    CheckResult<int32_t, int32_t, int32_t>(test, res, a, b);
}

//
// TEST_F(ListIRBuilderTest, list_int16_at_test) {
//    int8_t* ptr = NULL;
//    std::vector<fesql::storage::Row> rows;
//    BuildWindow2(rows, &ptr);
//    RunListAtCase<int16_t>(2, ::fesql::type::kInt16, "col2", ptr, 0);
//    RunListAtCase<int16_t>(22, ::fesql::type::kInt16, "col2", ptr, 1);
//    RunListAtCase<int16_t>(22222, ::fesql::type::kInt16, "col2", ptr, 4);
//    RunListAtCase<int16_t>(2222, ::fesql::type::kInt16, "col2", ptr, 3);
//    RunListAtCase<int16_t>(222, ::fesql::type::kInt16, "col2", ptr, 2);
//    free(ptr);
//}
TEST_F(FnIRBuilderTest, test_add_int32) {
    const std::string test =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend";
    CheckResult(test, 4, 1, 2);
}

TEST_F(FnIRBuilderTest, test_sub_int32) {
    const std::string test =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a-b\n    d=c+1\n    return "
        "d\nend";
    CheckResult(test, 0, 1, 2);
}

TEST_F(FnIRBuilderTest, test_bracket_int32) {
    const std::string test =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a*(b+1)\n    return c\nend";
    CheckResult(test, 3, 1, 2);
}

TEST_F(FnIRBuilderTest, test_mutable_variable_test) {
    const std::string test =
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    sum = 0\n"
        "    sum = sum + x\n"
        "    sum = sum + y\n"
        "    sum = sum + 1\n"
        "    return sum\n"
        "end";

    CheckResult(test, 6, 2, 3);
}
TEST_F(FnIRBuilderTest, test_if_block) {
    const std::string test =
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    if x > 1\n"
        "    \treturn x+y\n"
        "    return x*y\n"
        "end";

    CheckResult(test, 5, 2, 3);
    CheckResult(test, 3, 1, 3);
    CheckResult(test, 2, 1, 2);
}

TEST_F(FnIRBuilderTest, test_if_else_block) {
    const std::string test =
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    if x > 1\n"
        "    \treturn x+y\n"
        "    else\n"
        "    \treturn x*y\n"
        "end";

    CheckResult(test, 5, 2, 3);
    CheckResult(test, 3, 1, 3);
    CheckResult(test, 2, 1, 2);
}

TEST_F(FnIRBuilderTest, test_if_elif_else_block) {
    const std::string test =
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    if x > 1\n"
        "    \treturn x+y\n"
        "    elif y >2\n"
        "    \treturn x-y\n"
        "    else\n"
        "    \treturn x*y\n"
        "end";

    CheckResult(test, 5, 2, 3);
    CheckResult(test, -2, 1, 3);
    CheckResult(test, 2, 1, 2);
}

TEST_F(FnIRBuilderTest, test_if_else_block_redundant_ret) {
    const std::string test =
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    if x > 1\n"
        "    \treturn x+y\n"
        "    elif y >2\n"
        "    \treturn x-y\n"
        "    else\n"
        "    \treturn x*y\n"
        "    return 0\n"
        "end";

    CheckResult(test, 5, 2, 3);
    CheckResult(test, -2, 1, 3);
    CheckResult(test, 2, 1, 2);
}

TEST_F(FnIRBuilderTest, test_if_else_mutable_var_block) {
    const std::string test =
        "%%fun\n"
        "def test(x:i32,y:i32):i32\n"
        "    ret = 0\n"
        "    if x > 1\n"
        "    \tret = x+y\n"
        "    elif y >2\n"
        "    \tret = x-y\n"
        "    else\n"
        "    \tret = x*y\n"
        "    return ret\n"
        "end";

    CheckResult(test, 5, 2, 3);
    CheckResult(test, -2, 1, 3);
    CheckResult(test, 2, 1, 2);
}
TEST_F(FnIRBuilderTest, test_list_at_pos) {
    const std::string test =
        "%%fun\n"
        "def test(l:list<i32>, pos:i32):i32\n"
        "    return l[pos]\n"
        "end";

    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
    fesql::storage::ListV<int32_t> list(vec);
    fesql::storage::ListRef list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckResult<int32_t, fesql::storage::ListRef *, int32_t>(test, 1, &list_ref,
                                                             0);
    //    CheckResult(test, 3, &list, 1);
    //    CheckResult(test, 5, &list, 2);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
