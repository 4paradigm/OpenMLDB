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

#include "codegen/variable_ir_builder.h"
#include <memory>
#include <utility>
#include <vector>
#include "codegen/ir_base_builder.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT
ExitOnError ExitOnErr;
namespace hybridse {
namespace codegen {
class VariableIRBuilderTest : public ::testing::Test {
 public:
    VariableIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~VariableIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <class V1>
void MutableVariableCheck(::hybridse::node::DataType type, V1 value1, V1 result) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("predicate_func", *ctx);
    llvm::Type *llvm_type = NULL;
    ASSERT_TRUE(::hybridse::codegen::GetLLVMType(m.get(), type, &llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn =
        Function::Create(FunctionType::get(llvm_type, {llvm_type}, false),
                         Function::ExternalLinkage, "load_fn", m.get());

    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    ScopeVar scope_var;
    scope_var.AddVar("a", NativeValue::Create(arg0));
    VariableIRBuilder ir_builder(entry_block, &scope_var);
    NativeValue output;
    base::Status status;

    ASSERT_TRUE(
        ir_builder.StoreValue("x", NativeValue::Create(arg0), false, status));
    ASSERT_TRUE(ir_builder.LoadValue("x", &output, status));
    builder.CreateRet(output.GetValue(&builder));
    m->print(::llvm::errs(), NULL, true, true);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    V1 (*decode)(V1) = (V1(*)(V1))load_fn_jit.getAddress();
    V1 ret = decode(value1);
    ASSERT_EQ(ret, result);
}

template <class V1>
void ArrayVariableCheck(node::DataType type, V1 *array, int pos, V1 exp) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("array_func", *ctx);
    llvm::Type *element_type = NULL;
    ASSERT_TRUE(::hybridse::codegen::GetLLVMType(m.get(), type, &element_type));
    //    ::llvm::ArrayType *llvm_type =
    //        ::llvm::ArrayType::get(element_type, element_num);

    ::llvm::Type *llvm_type = element_type->getPointerTo();
    ASSERT_TRUE(nullptr != llvm_type);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn =
        Function::Create(FunctionType::get(element_type, {llvm_type}, false),
                         Function::ExternalLinkage, "load_fn", m.get());

    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    ScopeVar scope_var;
    scope_var.AddVar("array_arg", NativeValue::Create(arg0));
    VariableIRBuilder ir_builder(entry_block, &scope_var);
    NativeValue output;
    base::Status status;
    ASSERT_TRUE(ir_builder.LoadValue("array_arg", &output, status));

    llvm::Value *output_elem = nullptr;
    ASSERT_TRUE(
        ir_builder.LoadArrayIndex("array_arg", pos, &output_elem, status));
    builder.CreateRet(output_elem);
    m->print(::llvm::errs(), NULL, true, true);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    V1 (*decode)(V1[]) = (V1(*)(V1 *))load_fn_jit.getAddress();
    V1 ret = decode(array);
    ASSERT_EQ(ret, exp);
}

TEST_F(VariableIRBuilderTest, test_mutable_variable_assign) {
    MutableVariableCheck<int32_t>(::hybridse::node::DataType::kInt32, 999, 999);
    MutableVariableCheck<int64_t>(::hybridse::node::DataType::kInt64, 99999999L,
                                  99999999L);
    MutableVariableCheck<float>(::hybridse::node::DataType::kFloat, 0.999f,
                                0.999f);
    MutableVariableCheck<double>(::hybridse::node::DataType::kDouble, 0.999,
                                 0.999);
    MutableVariableCheck<int16_t>(::hybridse::node::DataType::kInt16, 99, 99);
}

TEST_F(VariableIRBuilderTest, test_int32_array_variable) {
    int len = 10;
    int32_t *int_num = new int32_t[len];
    for (int j = 0; j < len; ++j) {
        int_num[j] = j + 1;
    }
    ArrayVariableCheck<int32_t>(::hybridse::node::DataType::kInt32, int_num, 0, 1);
    ArrayVariableCheck<int32_t>(::hybridse::node::DataType::kInt32, int_num, 1, 2);
    ArrayVariableCheck<int32_t>(::hybridse::node::DataType::kInt32, int_num, 9,
                                10);
}

TEST_F(VariableIRBuilderTest, test_int8ptr_array_variable) {
    std::vector<std::string> strs = {"abcd", "efg", "hijk", "lmn",
                                     "opq",  "rst", "uvw",  "xyz"};
    int8_t **int_num = new int8_t *[strs.size()];
    for (size_t j = 0; j < strs.size(); ++j) {
        int_num[j] = reinterpret_cast<int8_t *>(&(strs[j]));
    }
    ArrayVariableCheck<int8_t *>(::hybridse::node::DataType::kInt8Ptr, int_num, 0,
                                 reinterpret_cast<int8_t *>(&strs[0]));
    ArrayVariableCheck<int8_t *>(::hybridse::node::DataType::kInt8Ptr, int_num, 1,
                                 reinterpret_cast<int8_t *>(&strs[1]));
    ArrayVariableCheck<int8_t *>(::hybridse::node::DataType::kInt8Ptr, int_num, 7,
                                 reinterpret_cast<int8_t *>(&strs[7]));
}

}  // namespace codegen
}  // namespace hybridse
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
