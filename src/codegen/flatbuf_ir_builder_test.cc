/*
 * flatbuf_ir_builder_test.cc
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

#include "codegen/flatbuf_ir_builder.h"
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
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

::flatbuffers::Offset<::flatbuffers::Table> Decode(flatbuffers::FlatBufferBuilder& builder) {
    ::flatbuffers::uoffset_t start = builder.StartTable();
    builder.AddElement<float>(4, 1.0f, 0.0f);
    builder.AddElement<int16_t>(6, 16, 0);
    builder.AddElement<int32_t>(8, 32, 0);
    builder.AddElement<int64_t>(10, 64, 0);
    builder.AddElement<double>(12, 2.0, 0);
    ::flatbuffers::uoffset_t end = builder.EndTable(start);
    return flatbuffers::Offset<flatbuffers::Table>(end);
}

void GetSchema(::fesql::type::TableDef& table) {
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("col1");
        column->set_type(::fesql::type::kFloat);
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("col2");
        column->set_type(::fesql::type::kInt16);
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("col3");
        column->set_type(::fesql::type::kInt32);
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("col4");
        column->set_type(::fesql::type::kInt64);
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_name("col5");
        column->set_type(::fesql::type::kDouble);
    }
    table.set_name("t1");
}

class FlatBufIRBuilderTest : public ::testing::Test {

public:
    FlatBufIRBuilderTest() {}
    ~FlatBufIRBuilderTest() {}
};

::llvm::Type* Convert(::llvm::LLVMContext& ctx, ::fesql::type::Type& type) {
    llvm::Type* field_type = NULL;
    switch (type) {
        case ::fesql::type::kBool:
            {
                field_type = ::llvm::Type::getInt1Ty(ctx);
                break;
            }
        case ::fesql::type::kInt16:
            {
                field_type = ::llvm::Type::getInt16Ty(ctx);
                break;
            }
        case ::fesql::type::kInt32:
            {
                field_type = ::llvm::Type::getInt32Ty(ctx);
                break;
            }
        case ::fesql::type::kInt64:
            {
                field_type = ::llvm::Type::getInt64Ty(ctx);
                break;
            }
        case ::fesql::type::kFloat:
            {
                field_type = ::llvm::Type::getFloatTy(ctx);
                break;
            }
        case ::fesql::type::kDouble:
            {
                field_type = ::llvm::Type::getDoubleTy(ctx);
                break;
            }
        default:
            {
                break;
            }
    }
    return field_type;
}
template<typename T> 
void RunGetField(const std::string& name, 
        T expect, ::fesql::type::Type& type) {
    ::fesql::type::TableDef table;
    GetSchema(table);
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_get_table", *ctx);
    auto J = ExitOnErr(LLJITBuilder().create());
    ::llvm::Type* ltype = Convert(*ctx, type);
    Function *fn =
    Function::Create(FunctionType::get(ltype,
                                         {Type::getInt8PtrTy(*ctx)}, 
                                         false),
                       Function::ExternalLinkage, "fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*fn->arg_begin(); 
    FlatBufDecodeIRBuilder fbuilder(&table);
    Value *table_start_offset = NULL;
    bool ok = fbuilder.BuildGetTableOffset(builder, arg0, *ctx, &table_start_offset);
    ASSERT_TRUE(ok);
    Value *vtable_start_offset = NULL;
    ok = fbuilder.BuildGetVTable(builder, arg0, table_start_offset, *ctx, &vtable_start_offset);
    ASSERT_TRUE(ok);
    Value *field_offset = NULL;
    ok = fbuilder.BuildGetOptionalFieldOffset(builder, arg0, *ctx, vtable_start_offset, name, &field_offset);
    ASSERT_TRUE(ok);
    Value *field_val = NULL;
    ok = fbuilder.BuildGetField(builder, arg0, table_start_offset, *ctx, field_offset, name, &field_val);
    ASSERT_TRUE(ok);
    builder.CreateRet(field_val);
    m->print(::llvm::errs(), NULL);
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto fn_jit = ExitOnErr(J->lookup("fn"));
    T (*decode)(int8_t*) = (T(*)(int8_t*))fn_jit.getAddress();
    flatbuffers::FlatBufferBuilder fb;
    ::flatbuffers::Offset<::flatbuffers::Table> row = Decode(fb);
    fb.Finish(row);
    int8_t* ptr = (int8_t*)fb.GetBufferPointer();
    T ret = decode(ptr);
    ASSERT_EQ(ret, expect);
}

TEST_F(FlatBufIRBuilderTest, test_get_table) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_get_table", *ctx);
    auto J = ExitOnErr(LLJITBuilder().create());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function *load_fn =
    Function::Create(FunctionType::get(Type::getInt32Ty(*ctx),
                                         {Type::getInt8PtrTy(*ctx)}, 
                                         false),
                       Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin(); 
    ::fesql::type::TableDef table;
    GetSchema(table);
    FlatBufDecodeIRBuilder fbuilder(&table);
    Value *table_start_offset = NULL;
    bool ok = fbuilder.BuildGetTableOffset(builder, arg0, *ctx, &table_start_offset);
    ASSERT_TRUE(ok);
    builder.CreateRet(table_start_offset);
    m->print(::llvm::errs(), NULL);
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    int32_t (*decode)(int8_t*) = (int32_t(*)(int8_t*))load_fn_jit.getAddress();
    flatbuffers::FlatBufferBuilder fb;
    ::flatbuffers::Offset<::flatbuffers::Table> row = Decode(fb);
    fb.Finish(row);
    int8_t* ptr = (int8_t*)fb.GetBufferPointer();
    int32_t ret = decode(ptr);
    ASSERT_EQ(ret, *((int32_t*)ptr));
    std::cout<< ret << std::endl;
}

TEST_F(FlatBufIRBuilderTest, test_get_vtable) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_get_vtable", *ctx);
    auto J = ExitOnErr(LLJITBuilder().create());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function *load_fn =
    Function::Create(FunctionType::get(Type::getInt32Ty(*ctx),
                                         {Type::getInt8PtrTy(*ctx)}, 
                                         false),
                       Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin(); 
    ::fesql::type::TableDef table;
    GetSchema(table);
    FlatBufDecodeIRBuilder fbuilder(&table);
    Value *table_start_offset = NULL;
    bool ok = fbuilder.BuildGetTableOffset(builder, arg0, *ctx, &table_start_offset);
    ASSERT_TRUE(ok);
    Value *vtable_start_offset = NULL;
    ok = fbuilder.BuildGetVTable(builder, arg0, table_start_offset, *ctx, &vtable_start_offset);
    ASSERT_TRUE(ok);
    builder.CreateRet(vtable_start_offset);
    m->print(::llvm::errs(), NULL);
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    int32_t (*decode)(int8_t*) = (int32_t(*)(int8_t*))load_fn_jit.getAddress();
    flatbuffers::FlatBufferBuilder fb;
    ::flatbuffers::Offset<::flatbuffers::Table> row = Decode(fb);
    fb.Finish(row);
    int8_t* ptr = (int8_t*)fb.GetBufferPointer();
    int32_t ret = decode(ptr);
    ::flatbuffers::uoffset_t raw_table_start = *((::flatbuffers::uoffset_t*)ptr);
    ::flatbuffers::uoffset_t raw_vtable_start = raw_table_start - *((::flatbuffers::soffset_t*)(ptr + raw_table_start));
    ASSERT_EQ(ret, raw_vtable_start);
    std::cout<< ret << std::endl;
}

TEST_F(FlatBufIRBuilderTest, test_get_float_field_offset) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_get_vtable", *ctx);
    auto J = ExitOnErr(LLJITBuilder().create());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function *load_fn =
    Function::Create(FunctionType::get(Type::getInt16Ty(*ctx),
                                         {Type::getInt8PtrTy(*ctx)}, 
                                         false),
                       Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin(); 
    ::fesql::type::TableDef table;
    GetSchema(table);
    FlatBufDecodeIRBuilder fbuilder(&table);
    Value *table_start_offset = NULL;
    bool ok = fbuilder.BuildGetTableOffset(builder, arg0, *ctx, &table_start_offset);
    ASSERT_TRUE(ok);
    Value *vtable_start_offset = NULL;
    ok = fbuilder.BuildGetVTable(builder, arg0, table_start_offset, *ctx, &vtable_start_offset);
    ASSERT_TRUE(ok);
    Value *field_offset = NULL;
    ok = fbuilder.BuildGetOptionalFieldOffset(builder, arg0, *ctx, vtable_start_offset, "col1", &field_offset);
    ASSERT_TRUE(ok);
    builder.CreateRet(field_offset);
    m->print(::llvm::errs(), NULL);
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    int16_t (*decode)(int8_t*) = (int16_t(*)(int8_t*))load_fn_jit.getAddress();
    flatbuffers::FlatBufferBuilder fb;
    ::flatbuffers::Offset<::flatbuffers::Table> row = Decode(fb);
    fb.Finish(row);
    int8_t* ptr = (int8_t*)fb.GetBufferPointer();
    int16_t ret = decode(ptr);
    ASSERT_EQ(ret, 32);
}


TEST_F(FlatBufIRBuilderTest, test_get_float_field) {
    std::string col = "col1";
    ::fesql::type::Type type = ::fesql::type::kFloat;
    RunGetField<float>(col, 1.0f, type);
}

TEST_F(FlatBufIRBuilderTest, test_get_int16_field) {
    std::string col2 = "col2";
    ::fesql::type::Type type = ::fesql::type::kInt16;
    RunGetField<int16_t>(col2, 16, type);
}

TEST_F(FlatBufIRBuilderTest, test_get_int32_field) {
    std::string col = "col3";
    ::fesql::type::Type type = ::fesql::type::kInt32;
    RunGetField<int32_t>(col, 32, type);
}

TEST_F(FlatBufIRBuilderTest, test_get_int64_field) {
    std::string col = "col4";
    ::fesql::type::Type type = ::fesql::type::kInt64;
    RunGetField<int64_t>(col, 64, type);
}

TEST_F(FlatBufIRBuilderTest, test_get_double_field) {
    std::string col = "col5";
    ::fesql::type::Type type = ::fesql::type::kDouble;
    RunGetField<double>(col, 2.0, type);
}
} // namespace of codegen
} // namespace of fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}



