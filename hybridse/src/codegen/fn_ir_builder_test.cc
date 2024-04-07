/*
 * Copyright 2021 4Paradigm
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
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/sql_compiler.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

ExitOnError ExitOnErr;

namespace hybridse {
namespace codegen {

class FnIRBuilderTest : public ::testing::Test {
 public:
    FnIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~FnIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <class R, class V1, class V2>
void CheckResult(node::FnNodeFnDef *fn_def, R exp, V1 a, V2 b) {
    base::Status status;
    node::NodeManager manager;
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("custom_fn", *ctx);
    FnIRBuilder fn_ir_builder(m.get());
    LOG(INFO) << *fn_def;
    ::llvm::Function *func = nullptr;
    bool ok = fn_ir_builder.Build(fn_def, &func, status);
    ASSERT_TRUE(ok);
    m->print(::llvm::errs(), NULL, true, true);
    LOG(INFO) << "before opt with ins cnt " << m->getInstructionCount();
    ::llvm::legacy::FunctionPassManager fpm(m.get());
    fpm.add(::llvm::createPromoteMemoryToRegisterPass());
    fpm.doInitialization();
    ::llvm::Module::iterator it;
    ::llvm::Module::iterator end = m->end();
    for (it = m->begin(); it != end; ++it) {
        fpm.run(*it);
    }
    LOG(INFO) << "after opt with ins cnt " << m->getInstructionCount();
    m->print(::llvm::errs(), NULL, true, true);
    auto jit = std::unique_ptr<vm::HybridSeJitWrapper>(vm::HybridSeJitWrapper::Create());
    jit->Init();
    ASSERT_TRUE(jit->AddModule(std::move(m), std::move(ctx)));
    auto test_fn = (R(*)(V1, V2))jit->FindFunction(fn_def->header_->GeIRFunctionName());
    R result = test_fn(a, b);
    LOG(INFO) << "exp: " << std::to_string(exp) << ", result: " << std::to_string(result);
    ASSERT_EQ(exp, result);
}

void CheckResult(node::FnNodeFnDef *fn_def, int32_t res, int32_t a, int32_t b) {
    CheckResult<int32_t, int32_t, int32_t>(fn_def, res, a, b);
}
//
// TEST_F(FnIRBuilderTest, test_add_int32) {
//    const std::string test =
//        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
//        "d\nend";
//    CheckResult(test, 4, 1, 2);
//}
//
// TEST_F(FnIRBuilderTest, test_sub_int32) {
//    const std::string test =
//        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a-b\n    d=c+1\n    return "
//        "d\nend";
//    CheckResult(test, 0, 1, 2);
//}
//
// TEST_F(FnIRBuilderTest, test_bracket_int32) {
//    const std::string test =
//        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a*(b+1)\n    return c\nend";
//    CheckResult(test, 3, 1, 2);
//}
//
// TEST_F(FnIRBuilderTest, test_mutable_variable_test) {
//    const std::string test =
//        "%%fun\n"
//        "def test(x:i32,y:i32):i32\n"
//        "    sum = 0\n"
//        "    sum = sum + x\n"
//        "    sum = sum + y\n"
//        "    sum = sum + 1\n"
//        "    return sum\n"
//        "end";
//
//    CheckResult(test, 6, 2, 3);
//}
// TEST_F(FnIRBuilderTest, test_if_block) {
//    const std::string test =
//        "%%fun\n"
//        "def test(x:i32,y:i32):i32\n"
//        "    if x > 1\n"
//        "    \treturn x+y\n"
//        "    return x*y\n"
//        "end";
//
//    CheckResult(test, 5, 2, 3);
//    CheckResult(test, 3, 1, 3);
//    CheckResult(test, 2, 1, 2);
//}
//
// TEST_F(FnIRBuilderTest, test_if_else_block) {
//    const std::string test =
//        "%%fun\n"
//        "def test(x:i32,y:i32):i32\n"
//        "    if x > 1\n"
//        "    \treturn x+y\n"
//        "    else\n"
//        "    \treturn x*y\n"
//        "end";
//
//    CheckResult(test, 5, 2, 3);
//    CheckResult(test, 3, 1, 3);
//    CheckResult(test, 2, 1, 2);
//}
//
// TEST_F(FnIRBuilderTest, test_if_elif_else_block) {
//    const std::string test =
//        "%%fun\n"
//        "def test(x:i32,y:i32):i32\n"
//        "    if x > 1\n"
//        "    \treturn x+y\n"
//        "    elif y >2\n"
//        "    \treturn x-y\n"
//        "    else\n"
//        "    \treturn x*y\n"
//        "end";
//
//    CheckResult(test, 5, 2, 3);
//    CheckResult(test, -2, 1, 3);
//    CheckResult(test, 2, 1, 2);
//}
//
// TEST_F(FnIRBuilderTest, test_if_else_block_redundant_ret) {
//    const std::string test =
//        "%%fun\n"
//        "def test(x:i32,y:i32):i32\n"
//        "    if x > 1\n"
//        "    \treturn x+y\n"
//        "    elif y >2\n"
//        "    \treturn x-y\n"
//        "    else\n"
//        "    \treturn x*y\n"
//        "    return 0\n"
//        "end";
//
//    CheckResult(test, 5, 2, 3);
//    CheckResult(test, -2, 1, 3);
//    CheckResult(test, 2, 1, 2);
//}
//
// TEST_F(FnIRBuilderTest, test_if_else_mutable_var_block) {
//    const std::string test =
//        "%%fun\n"
//        "def test(x:i32,y:i32):i32\n"
//        "    ret = 0\n"
//        "    if x > 1\n"
//        "    \tret = x+y\n"
//        "    elif y >2\n"
//        "    \tret = x-y\n"
//        "    else\n"
//        "    \tret = x*y\n"
//        "    return ret\n"
//        "end";
//
//    CheckResult(test, 5, 2, 3);
//    CheckResult(test, -2, 1, 3);
//    CheckResult(test, 2, 1, 2);
//}
//
///* TEST_F(FnIRBuilderTest, test_list_at_pos) {
//    const std::string test =
//        "%%fun\n"
//        "def test(l:list<i32>, pos:i32):i32\n"
//        "    return l[pos]\n"
//        "end";
//
//    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
//    hybridse::codec::ArrayListV<int32_t> list(&vec);
//    hybridse::codec::ListRef<> list_ref;
//    list_ref.list = reinterpret_cast<int8_t *>(&list);
// CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(test, 1, &list_ref, 0);
// CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(test, 3, &list_ref, 1);
// CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(test, 5, &list_ref, 2);
// CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(test, 7, &list_ref, 3);
// CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(test, 9, &list_ref, 4);
//}*/
//
TEST_F(FnIRBuilderTest, test_for_in_sum) {
    //    const std::string test =
    //        "%%fun\n"
    //        "def test(l:list<i32>, a:i32):i32\n"
    //        "    sum=0\n"
    //        "    for x in l\n"
    //        "        sum = sum + x\n"
    //        "    return sum\n"
    //        "end";

    node::FnNodeList *params = manager_->MakeFnListNode();
    params->AddChild(
        manager_->MakeFnParaNode("l", manager_->MakeTypeNode(node::kList, manager_->MakeTypeNode(node::kInt32))));
    params->AddChild(manager_->MakeFnParaNode("a", manager_->MakeTypeNode(node::kInt32)));

    node::FnNodeList *block = manager_->MakeFnListNode();
    block->AddChild(manager_->MakeAssignNode("sum", manager_->MakeConstNode(0)));
    node::FnForInBlock *for_in_block = manager_->MakeForInBlock(
        dynamic_cast<node::FnForInNode *>(manager_->MakeForInStmtNode("x", manager_->MakeUnresolvedExprId("l"))),
        manager_->MakeFnListNode(manager_->MakeAssignNode(
            "sum", manager_->MakeBinaryExprNode(manager_->MakeUnresolvedExprId("sum"),
                                                manager_->MakeUnresolvedExprId("x"), node::kFnOpAdd))));
    block->AddChild(for_in_block);
    block->AddChild(manager_->MakeReturnStmtNode(manager_->MakeUnresolvedExprId("sum")));

    node::FnNodeFnDef *fn_def = dynamic_cast<node::FnNodeFnDef *>(manager_->MakeFnDefNode(
        manager_->MakeFnHeaderNode("test", params, manager_->MakeTypeNode(node::kInt32)), block));

    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
    hybridse::codec::ArrayListV<int32_t> list(&vec);
    hybridse::codec::ListRef<> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(fn_def, 1 + 3 + 5 + 7 + 9, &list_ref, 0);
}
//
// TEST_F(FnIRBuilderTest, test_for_in_sum_ret) {
//    const std::string test =
//        "%%fun\n"
//        "def test(l:list<i32>, a:i32):i32\n"
//        "    sum=0\n"
//        "    for x in l\n"
//        "        if sum > 10\n"
//        "            return sum\n"
//        "        else\n"
//        "            sum = sum + x\n"
//        "    return sum\n"
//        "end";
//
//    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
//    hybridse::codec::ArrayListV<int32_t> list(&vec);
//    hybridse::codec::ListRef<> list_ref;
//    list_ref.list = reinterpret_cast<int8_t *>(&list);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 1 + 3 + 5 + 7, &list_ref, 0);
//}
//
// TEST_F(FnIRBuilderTest, test_for_in_condition_sum) {
//    const std::string test =
//        "%%fun\n"
//        "def test(l:list<i32>, a:i32):i32\n"
//        "    sum=0\n"
//        "    for x in l\n"
//        "        if x > a\n"
//        "            sum = sum + x\n"
//        "    return sum\n"
//        "end";
//
//    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
//    hybridse::codec::ArrayListV<int32_t> list(&vec);
//    hybridse::codec::ListRef<> list_ref;
//    list_ref.list = reinterpret_cast<int8_t *>(&list);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 1 + 3 + 5 + 7 + 9, &list_ref, 0);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 3 + 5 + 7 + 9, &list_ref, 1);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 3 + 5 + 7 + 9, &list_ref, 2);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(test, 5 + 7 + 9,
//                                                                &list_ref, 3);
//}
//
// TEST_F(FnIRBuilderTest, test_for_in_condition2_sum) {
//    const std::string test =
//        "%%fun\n"
//        "def test(l:list<i32>, a:i32):i32\n"
//        "    sum=0\n"
//        "    for x in l\n"
//        "        if x > a\n"
//        "            sum = sum + a\n"
//        "        elif x >0\n"
//        "            sum = sum + x\n"
//        "        else\n"
//        "            sum = sum - x\n"
//        "    return sum\n"
//        "end\n";
//
//    std::vector<int32_t> vec = {-4, -2, 1, 3, 5, 7, 9};
//    hybridse::codec::ArrayListV<int32_t> list(&vec);
//    hybridse::codec::ListRef<> list_ref;
//    list_ref.list = reinterpret_cast<int8_t *>(&list);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 4 + 2 + 1 + 3 + 5 + 5 + 5, &list_ref, 5);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 4 + 2 + 1 + 1 + 1 + 1 + 1, &list_ref, 1);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 4 + 2 + 1 + 2 + 2 + 2 + 2, &list_ref, 2);
//}
//
// TEST_F(FnIRBuilderTest, test_for_in_sum_add_assign) {
//    const std::string test =
//        "%%fun\n"
//        "def test(l:list<i32>, a:i32):i32\n"
//        "    sum=0\n"
//        "    for x in l\n"
//        "        sum += x\n"
//        "    return sum\n"
//        "end";
//
//    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
//    hybridse::codec::ArrayListV<int32_t> list(&vec);
//    hybridse::codec::ListRef<> list_ref;
//    list_ref.list = reinterpret_cast<int8_t *>(&list);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 1 + 3 + 5 + 7 + 9, &list_ref, 0);
//}
// TEST_F(FnIRBuilderTest, test_for_in_sum_minus_assign) {
//    const std::string test =
//        "%%fun\n"
//        "def test(l:list<i32>, a:i32):i32\n"
//        "    sum=0\n"
//        "    for x in l\n"
//        "        sum -= x\n"
//        "    return sum\n"
//        "end";
//
//    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
//    hybridse::codec::ArrayListV<int32_t> list(&vec);
//    hybridse::codec::ListRef<> list_ref;
//    list_ref.list = reinterpret_cast<int8_t *>(&list);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, -1 - 3 - 5 - 7 - 9, &list_ref, 0);
//}
//
// TEST_F(FnIRBuilderTest, test_for_in_sum_multi_assign) {
//    const std::string test =
//        "%%fun\n"
//        "def test(l:list<i32>, a:i32):i32\n"
//        "    sum=1\n"
//        "    for x in l\n"
//        "        sum *= x\n"
//        "    return sum\n"
//        "end";
//
//    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
//    hybridse::codec::ArrayListV<int32_t> list(&vec);
//    hybridse::codec::ListRef<> list_ref;
//    list_ref.list = reinterpret_cast<int8_t *>(&list);
//    CheckResult<int32_t, hybridse::codec::ListRef<> *, int32_t>(
//        test, 1 * 3 * 5 * 7 * 9, &list_ref, 0);
//}
//
// TEST_F(FnIRBuilderTest, test_for_in_sum_fdiv_assign) {
//    const std::string test =
//        "%%fun\n"
//        "def test(l:list<i32>, a:i32):double\n"
//        "    sum=1.0\n"
//        "    for x in l\n"
//        "        sum /= x\n"
//        "    return sum\n"
//        "end";
//
//    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
//    hybridse::codec::ArrayListV<int32_t> list(&vec);
//    hybridse::codec::ListRef<> list_ref;
//    list_ref.list = reinterpret_cast<int8_t *>(&list);
//    CheckResult<double, hybridse::codec::ListRef<> *, int32_t>(
//        test, 1.0 / 3.0 / 5.0 / 7.0 / 9.0, &list_ref, 0);
//}
}  // namespace codegen
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
