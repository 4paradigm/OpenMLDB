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

#include "codegen/buf_ir_builder.h"
#include <stdio.h>
#include <cstdlib>
#include <memory>
#include <vector>
#include "codegen/codegen_base_test.h"
#include "codegen/ir_base_builder.h"
#include "gtest/gtest.h"
#include "storage/codec.h"
#include "storage/type_native_fn.h"
#include "storage/window.h"

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

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

struct TestString {
    int32_t size;
    char* data;
};

void PrintInt16(int16_t val) { std::cout << "int16_" << val << std::endl; }

void PrintInt32(int32_t val) { std::cout << "int32" << val << std::endl; }

void PrintPtr(int8_t* ptr) { printf("ptr %p\n", ptr); }

void PrintString(int8_t* ptr) {
    TestString* ts = reinterpret_cast<TestString*>(ptr);
    std::string str(ts->data, ts->size);
    std::cout << "content " << str << std::endl;
}

template <class T>
T PrintList(int8_t* input) {
    T sum = 0;
    if (nullptr == input) {
        std::cout << "list is null" << std::endl;
    } else {
        std::cout << "list ptr is ok" << std::endl;
    }
    ::fesql::storage::ListRef* list_ref =
        reinterpret_cast<::fesql::storage::ListRef*>(input);
    ::fesql::storage::ColumnImpl<T>* column =
        reinterpret_cast<::fesql::storage::ColumnImpl<T>*>(list_ref->list);
    fesql::storage::IteratorImpl<T> iter(*column);
    ::fesql::storage::IteratorImpl<T>* col = &iter;
    std::cout << "[";
    while (col->Valid()) {
        T v = col->Next();
        std::cout << v << ",";
        sum += v;
    }
    std::cout << "]";
    return sum;
}

int16_t PrintListInt16(int8_t* input) { return PrintList<int16_t>(input); }
int32_t PrintListInt32(int8_t* input) { return PrintList<int32_t>(input); }
int64_t PrintListInt64(int8_t* input) { return PrintList<int64_t>(input); }
float PrintListFloat(int8_t* input) { return PrintList<float>(input); }
double PrintListDouble(int8_t* input) { return PrintList<double>(input); }
int32_t PrintListString(int8_t* input) {
    int32_t cnt = 0;
    if (nullptr == input) {
        std::cout << "list is null";
    } else {
        std::cout << "list ptr is ok" << std::endl;
    }
    ::fesql::storage::ListRef* list_ref =
        reinterpret_cast<::fesql::storage::ListRef*>(input);
    ::fesql::storage::StringColumnImpl* column =
        reinterpret_cast<::fesql::storage::StringColumnImpl*>(list_ref->list);
    fesql::storage::IteratorImpl<::fesql::storage::StringRef> iter(*column);
    ::fesql::storage::IteratorImpl<::fesql::storage::StringRef>* col = &iter;
    std::cout << "[";
    while (col->Valid()) {
        ::fesql::storage::StringRef v = col->Next();
        std::string str(v.data, v.size);
        std::cout << str << ", ";
        cnt++;
    }
    std::cout << "]";
    return cnt;
}

void AssertStrEq(int8_t* ptr) {
    TestString* ts = reinterpret_cast<TestString*>(ptr);
    ASSERT_EQ(1, ts->size);
    std::string str(ts->data, ts->size);
    ASSERT_EQ(str, "1");
}

namespace fesql {
namespace codegen {

class BufIRBuilderTest : public ::testing::Test {
 public:
    BufIRBuilderTest() {}
    ~BufIRBuilderTest() {}
};

void RunEncode(int8_t** output_ptr) {
    ::fesql::type::TableDef table;
    table.set_name("t1");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col1");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col2");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_encode", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function* fn = Function::Create(
        FunctionType::get(Type::getVoidTy(*ctx),
                          {Type::getInt8PtrTy(*ctx)->getPointerTo()}, false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    IRBuilder<> builder(entry_block);
    ScopeVar sv;
    sv.Enter("enter row scope");
    std::map<uint32_t, ::llvm::Value*> outputs;
    outputs.insert(std::make_pair(0, builder.getInt16(16)));
    outputs.insert(std::make_pair(1, builder.getInt32(32)));
    outputs.insert(std::make_pair(
        2, ::llvm::ConstantFP::get(*ctx, ::llvm::APFloat(32.1f))));
    outputs.insert(std::make_pair(
        3, ::llvm::ConstantFP::get(*ctx, ::llvm::APFloat(64.1))));
    outputs.insert(std::make_pair(4, builder.getInt64(64)));

    std::string hello = "hello";
    ::llvm::Value* string_ref = NULL;
    bool ok = GetConstFeString(hello, entry_block, &string_ref);
    ASSERT_TRUE(ok);
    outputs.insert(std::make_pair(5, string_ref));
    BufNativeEncoderIRBuilder buf_encoder_builder(&outputs, table.columns(),
                                                  entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;
    ok = buf_encoder_builder.BuildEncode(arg0);
    ASSERT_TRUE(ok);
    builder.CreateRetVoid();
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    ::fesql::storage::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    void (*decode)(int8_t**) =
        reinterpret_cast<void (*)(int8_t**)>(load_fn_jit.getAddress());
    decode(output_ptr);
}

template <class T>
void RunCaseV1(T expected, const ::fesql::type::Type& type,
               const std::string& col, int8_t* row, int32_t row_size) {
    ::fesql::type::TableDef table;
    table.set_name("t1");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_buf", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    bool is_void = false;
    ::llvm::Type* retTy = NULL;
    switch (type) {
        case ::fesql::type::kInt16:
            retTy = Type::getInt16Ty(*ctx);
            break;
        case ::fesql::type::kInt32:
            retTy = Type::getInt32Ty(*ctx);
            break;
        case ::fesql::type::kInt64:
            retTy = Type::getInt64Ty(*ctx);
            break;
        case ::fesql::type::kDouble:
            retTy = Type::getDoubleTy(*ctx);
            break;
        case ::fesql::type::kFloat:
            retTy = Type::getFloatTy(*ctx);
            break;
        default:
            is_void = true;
            retTy = Type::getVoidTy(*ctx);
    }
    Function* fn = Function::Create(
        FunctionType::get(
            retTy, {Type::getInt8PtrTy(*ctx), Type::getInt32Ty(*ctx)}, false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    ScopeVar sv;
    sv.Enter("enter row scope");
    BufNativeIRBuilder buf_builder(table.columns(), entry_block, &sv);
    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;
    ++it;
    Argument* arg1 = &*it;
    ::llvm::Value* val = NULL;
    bool ok = buf_builder.BuildGetField(col, arg0, arg1, &val);
    ASSERT_TRUE(ok);
    if (type == ::fesql::type::kVarchar) {
        ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
        ::llvm::Value* i8_ptr = builder.CreatePointerCast(val, i8_ptr_ty);
        ::llvm::Type* void_ty = builder.getVoidTy();
        ::llvm::FunctionCallee callee =
            m->getOrInsertFunction("print_str", void_ty, i8_ptr_ty);
        builder.CreateCall(callee, ::llvm::ArrayRef<Value*>(i8_ptr));
    }
    if (!is_void) {
        builder.CreateRet(val);
    } else {
        builder.CreateRetVoid();
    }
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    ::llvm::StringRef symbol1("print_str");
    ::llvm::StringRef symbol2("print_i16");
    ::llvm::StringRef symbol3("print_i32");
    ::llvm::StringRef symbol4("print_ptr");
    ::llvm::orc::SymbolMap symbol_map;
    ::llvm::JITEvaluatedSymbol jit_symbol1(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&AssertStrEq)),
        ::llvm::JITSymbolFlags());

    ::llvm::JITEvaluatedSymbol jit_symbol2(
        ::llvm::pointerToJITTargetAddress(reinterpret_cast<void*>(&PrintInt16)),
        ::llvm::JITSymbolFlags());

    ::llvm::JITEvaluatedSymbol jit_symbol3(
        ::llvm::pointerToJITTargetAddress(reinterpret_cast<void*>(&PrintInt32)),
        ::llvm::JITSymbolFlags());

    ::llvm::JITEvaluatedSymbol jit_symbol4(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&AssertStrEq)),
        ::llvm::JITSymbolFlags());

    symbol_map.insert(std::make_pair(mi(symbol1), jit_symbol1));
    symbol_map.insert(std::make_pair(mi(symbol2), jit_symbol2));
    symbol_map.insert(std::make_pair(mi(symbol3), jit_symbol3));
    symbol_map.insert(std::make_pair(mi(symbol4), jit_symbol4));
    // add codec

    auto err = jd.define(::llvm::orc::absoluteSymbols(symbol_map));
    if (err) {
        ASSERT_TRUE(false);
    }
    ::fesql::storage::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    if (!is_void) {
        T(*decode)
        (int8_t*, int32_t) =
            reinterpret_cast<T (*)(int8_t*, int32_t)>(load_fn_jit.getAddress());
        ASSERT_EQ(expected, decode(row, row_size));

    } else {
        void (*decode)(int8_t*, int32_t) =
            reinterpret_cast<void (*)(int8_t*, int32_t)>(
                load_fn_jit.getAddress());
        decode(row, row_size);
    }
}

template <class T>
void RunColCase(T expected, const ::fesql::type::Type& type,
                const std::string& col, int8_t* window) {
    ::fesql::type::TableDef table;
    table.set_name("t1");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_buf", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    bool is_void = false;
    ::llvm::Type* retTy = NULL;
    switch (type) {
        case ::fesql::type::kInt16:
            retTy = Type::getInt16Ty(*ctx);
            break;
        case ::fesql::type::kInt32:
            retTy = Type::getInt32Ty(*ctx);
            break;
        case ::fesql::type::kInt64:
            retTy = Type::getInt64Ty(*ctx);
            break;
        case ::fesql::type::kDouble:
            retTy = Type::getDoubleTy(*ctx);
            break;
        case ::fesql::type::kFloat:
            retTy = Type::getFloatTy(*ctx);
            break;
        case ::fesql::type::kVarchar:
            retTy = Type::getInt32Ty(*ctx);
            break;
        default:
            is_void = true;
            retTy = Type::getVoidTy(*ctx);
    }
    Function* fn = Function::Create(
        FunctionType::get(retTy, {Type::getInt8PtrTy(*ctx)}, false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    ScopeVar sv;
    sv.Enter("enter row scope");
    BufNativeIRBuilder buf_builder(table.columns(), entry_block, &sv);
    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;
    ::llvm::Value* val = NULL;
    bool ok = buf_builder.BuildGetCol(col, arg0, &val);
    ASSERT_TRUE(ok);

    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* i8_ptr = builder.CreatePointerCast(val, i8_ptr_ty);
    llvm::FunctionCallee callee;
    switch (type) {
        case fesql::type::kInt16:
            callee = m->getOrInsertFunction("print_list_i16", retTy, i8_ptr_ty);
            break;
        case fesql::type::kInt32:
            callee = m->getOrInsertFunction("print_list_i32", retTy, i8_ptr_ty);
            break;
        case fesql::type::kInt64:
            callee = m->getOrInsertFunction("print_list_i64", retTy, i8_ptr_ty);
            break;
        case fesql::type::kFloat:
            callee =
                m->getOrInsertFunction("print_list_float", retTy, i8_ptr_ty);
            break;
        case fesql::type::kDouble:
            callee =
                m->getOrInsertFunction("print_list_double", retTy, i8_ptr_ty);
            break;
        case fesql::type::kVarchar:
            callee =
                m->getOrInsertFunction("print_list_string", retTy, i8_ptr_ty);
            break;

        default: {
            return;
        }
    }
    ::llvm::Value* ret_val =
        builder.CreateCall(callee, ::llvm::ArrayRef<Value*>(i8_ptr));
    if (!is_void) {
        builder.CreateRet(ret_val);
    } else {
        builder.CreateRetVoid();
    }
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    ::llvm::StringRef symbol1("print_list_i16");
    ::llvm::StringRef symbol2("print_list_i32");
    ::llvm::StringRef symbol3("print_list_i64");
    ::llvm::StringRef symbol4("print_list_float");
    ::llvm::StringRef symbol5("print_list_double");
    ::llvm::StringRef symbol6("print_list_string");
    ::llvm::orc::SymbolMap symbol_map;

    ::llvm::JITEvaluatedSymbol jit_symbol1(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&PrintListInt16)),
        ::llvm::JITSymbolFlags());
    ::llvm::JITEvaluatedSymbol jit_symbol2(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&PrintListInt32)),
        ::llvm::JITSymbolFlags());

    ::llvm::JITEvaluatedSymbol jit_symbol3(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&PrintListInt64)),
        ::llvm::JITSymbolFlags());
    ::llvm::JITEvaluatedSymbol jit_symbol4(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&PrintListFloat)),
        ::llvm::JITSymbolFlags());
    ::llvm::JITEvaluatedSymbol jit_symbol5(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&PrintListDouble)),
        ::llvm::JITSymbolFlags());

    ::llvm::JITEvaluatedSymbol jit_symbol6(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&PrintListString)),
        ::llvm::JITSymbolFlags());

    symbol_map.insert(std::make_pair(mi(symbol1), jit_symbol1));
    symbol_map.insert(std::make_pair(mi(symbol2), jit_symbol2));
    symbol_map.insert(std::make_pair(mi(symbol3), jit_symbol3));
    symbol_map.insert(std::make_pair(mi(symbol4), jit_symbol4));
    symbol_map.insert(std::make_pair(mi(symbol5), jit_symbol5));
    symbol_map.insert(std::make_pair(mi(symbol6), jit_symbol6));
    // add codec

    auto err = jd.define(::llvm::orc::absoluteSymbols(symbol_map));
    if (err) {
        ASSERT_TRUE(false);
    }
    ::fesql::storage::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    if (!is_void) {
        T(*decode)
        (int8_t*) = reinterpret_cast<T (*)(int8_t*)>(load_fn_jit.getAddress());
        ASSERT_EQ(expected, decode(window));

    } else {
        void (*decode)(int8_t*) =
            reinterpret_cast<void (*)(int8_t*)>(load_fn_jit.getAddress());
        decode(window);
    }
}

TEST_F(BufIRBuilderTest, native_test_load_int16) {
    int8_t* ptr = NULL;
    uint32_t size = 0;
    BuildBuf(&ptr, &size);
    RunCaseV1<int16_t>(16, ::fesql::type::kInt16, "col2", ptr, size);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_string) {
    int8_t* ptr = NULL;
    uint32_t size = 0;
    BuildBuf(&ptr, &size);
    RunCaseV1<int16_t>(16, ::fesql::type::kVarchar, "col6", ptr, size);
    free(ptr);
}

TEST_F(BufIRBuilderTest, encode_ir_builder) {
    int8_t* ptr = NULL;
    RunEncode(&ptr);
    bool ok = ptr != NULL;
    ASSERT_TRUE(ok);
    uint32_t size = *reinterpret_cast<uint32_t*>(ptr + 2);
    ASSERT_EQ(size, 39);
    ASSERT_EQ(16, *reinterpret_cast<int16_t*>(ptr + 7));
    ASSERT_EQ(32, *reinterpret_cast<int32_t*>(ptr + 9));
    ASSERT_EQ(32.1f, *reinterpret_cast<float*>(ptr + 13));
    ASSERT_EQ(64.1, *reinterpret_cast<double*>(ptr + 17));
    ASSERT_EQ(64, *reinterpret_cast<int64_t*>(ptr + 25));
    ASSERT_EQ(34, *reinterpret_cast<int8_t*>(ptr + 33));
    char* data = reinterpret_cast<char*>(ptr + 34);
    std::string val(data, 39 - 34);
    ASSERT_EQ("hello", val);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_int16_col) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow(rows, &ptr);
    RunColCase<int16_t>(16 * 5, ::fesql::type::kInt16, "col2", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_int32_col) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow(rows, &ptr);
    RunColCase<int32_t>(32 * 5, ::fesql::type::kInt32, "col1", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_int64_col) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow(rows, &ptr);
    RunColCase<int64_t>(64 * 5, ::fesql::type::kInt64, "col5", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_float_col) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow(rows, &ptr);
    RunColCase<float>(2.1f * 5, ::fesql::type::kFloat, "col3", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_double_col) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow(rows, &ptr);
    RunColCase<double>(3.1f * 5, ::fesql::type::kDouble, "col4", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_string_col) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow(rows, &ptr);
    RunColCase<int32_t>(5, ::fesql::type::kVarchar, "col6", ptr);
    free(ptr);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
