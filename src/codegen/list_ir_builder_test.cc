/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * list_ir_builder_test.cc
 *
 * Author: chenjing
 * Date: 2020/2/14
 *--------------------------------------------------------------------------
 **/

#include "codegen/list_ir_builder.h"
#include <stdio.h>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/buf_ir_builder.h"
#include "codegen/codegen_base_test.h"
#include "codegen/ir_base_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "codegen/window_ir_builder.h"
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
#include "udf/udf.h"
#include "vm/sql_compiler.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

using fesql::base::ConstIterator;
struct TestString {
    int32_t size;
    char* data = nullptr;
};

void AssertStrEq(std::string exp, int8_t* ptr) {
    ::fesql::codec::StringRef* ts =
        reinterpret_cast<::fesql::codec::StringRef*>(ptr);
    ASSERT_EQ(exp.size(), ts->size);
    std::string str(ts->data, ts->size);
    ASSERT_EQ(str, exp);
}

namespace fesql {
namespace codegen {

using fesql::codec::ListV;
using fesql::codec::Row;
class ListIRBuilderTest : public ::testing::Test {
 public:
    ListIRBuilderTest() {}
    ~ListIRBuilderTest() {}
};

template <class V>
V IteratorSum(int8_t* input) {
    if (nullptr == input) {
        std::cout << "iter is null" << std::endl;
    } else {
        std::cout << "iter ptr is ok" << std::endl;
    }
    V result = 0;
    ::fesql::codec::IteratorRef* iter_ref =
        (::fesql::codec::IteratorRef*)(input);
    ConstIterator<uint64_t, V>* iter =
        (ConstIterator<uint64_t, V>*)(iter_ref->iterator);
    while (iter->Valid()) {
        result += iter->GetValue();
        iter->Next();
    }
    return result;
}

codec::Timestamp IteratorSumTimestamp(int8_t* input) {
    if (nullptr == input) {
        std::cout << "iter is null" << std::endl;
    } else {
        std::cout << "iter ptr is ok" << std::endl;
    }
    int64_t result = 0;
    ::fesql::codec::IteratorRef* iter_ref =
        (::fesql::codec::IteratorRef*)(input);
    ConstIterator<uint64_t, codec::Timestamp>* iter =
        (ConstIterator<uint64_t, codec::Timestamp>*)(iter_ref->iterator);
    while (iter->Valid()) {
        result += iter->GetValue().ts_;
        iter->Next();
    }
    return codec::Timestamp(result);
}
int16_t IteratorSumInt16(int8_t* input) { return IteratorSum<int16_t>(input); }
int32_t IteratorSumInt32(int8_t* input) { return IteratorSum<int32_t>(input); }
int64_t IteratorSumInt64(int8_t* input) { return IteratorSum<int64_t>(input); }
float IteratorSumFloat(int8_t* input) { return IteratorSum<float>(input); }
double IteratorSumDouble(int8_t* input) { return IteratorSum<double>(input); }

template <class T>
void GetListAtPos(const type::TableDef& table, T* result,
                  const ::fesql::type::Type& type, const std::string& col,
                  int8_t* window, int32_t pos) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_buf", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
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
            retTy = Type::getInt8PtrTy(*ctx);
            break;
        case ::fesql::type::kTimestamp: {
            node::TypeNode type_node(fesql::node::kTimestamp);
            ASSERT_TRUE(codegen::GetLLVMType(m.get(), &type_node, &retTy));
            break;
        }
        default:
            LOG(WARNING) << "invalid test type";
            FAIL();
    }
    Function* fn = Function::Create(
        FunctionType::get(::llvm::Type::getVoidTy(*ctx),
                          {Type::getInt8PtrTy(*ctx), Type::getInt32Ty(*ctx),
                           retTy->getPointerTo()},
                          false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    ScopeVar sv;
    sv.Enter("enter row scope");
    MemoryWindowDecodeIRBuilder buf_builder(table.columns(), entry_block);
    ListIRBuilder list_builder(entry_block, &sv);
    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;
    it++;
    Argument* arg1 = &*it;
    it++;
    Argument* arg2 = &*it;

    // build column
    ::llvm::Value* column = NULL;
    bool ok = buf_builder.BuildGetCol(col, arg0, &column);
    ASSERT_TRUE(ok);

    ::llvm::Value* val = nullptr;
    base::Status status;
    ASSERT_TRUE(list_builder.BuildAt(column, arg1, &val, status));

    switch (type) {
        case type::kTimestamp: {
            codegen::TimestampIRBuilder timestamp_builder;
            ::llvm::Value* ts_output;
            ASSERT_TRUE(timestamp_builder.GetTs(builder.GetInsertBlock(), val,
                                                &ts_output));
            ASSERT_TRUE(timestamp_builder.SetTs(builder.GetInsertBlock(), arg2,
                                                ts_output));
            break;
        }
        default: {
            builder.CreateStore(val, arg2);
        }
    }
    builder.CreateRetVoid();

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::udf::InitUDFSymbol(jd, mi);
    // add codec
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    void (*decode)(int8_t*, int32_t, T*) =
        reinterpret_cast<void (*)(int8_t*, int32_t, T*)>(
            load_fn_jit.getAddress());
    decode(window, pos, result);
}
template <class T>
void GetListIterator(T expected, const type::TableDef& table,
                     const ::fesql::type::Type& type, const std::string& col,
                     int8_t* window) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_buf", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
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
            retTy = Type::getInt8PtrTy(*ctx);
            break;
        case ::fesql::type::kTimestamp:
            ASSERT_TRUE(GetLLVMType(m.get(), node::kTimestamp, &retTy));
            break;
        default:
            LOG(WARNING) << "invalid test type";
            FAIL();
    }
    Function* fn = Function::Create(
        FunctionType::get(retTy, {Type::getInt8PtrTy(*ctx)}, false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    ScopeVar sv;
    sv.Enter("enter row scope");

    MemoryWindowDecodeIRBuilder buf_builder(table.columns(), entry_block);
    ListIRBuilder list_builder(entry_block, &sv);

    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;

    // build column
    ::llvm::Value* column = NULL;
    bool ok = buf_builder.BuildGetCol(col, arg0, &column);
    ASSERT_TRUE(ok);

    ::llvm::Value* iterator = nullptr;
    base::Status status;
    ASSERT_TRUE(list_builder.BuildIterator(column, &iterator, status));
    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* i8_ptr = builder.CreatePointerCast(iterator, i8_ptr_ty);
    llvm::FunctionCallee callee;
    switch (type) {
        case fesql::type::kInt16:
            callee =
                m->getOrInsertFunction("iterator_sum_i16", retTy, i8_ptr_ty);
            break;
        case fesql::type::kInt32:
            callee =
                m->getOrInsertFunction("iterator_sum_i32", retTy, i8_ptr_ty);
            break;
        case fesql::type::kInt64:
            callee =
                m->getOrInsertFunction("iterator_sum_i64", retTy, i8_ptr_ty);
            break;
        case fesql::type::kFloat:
            callee =
                m->getOrInsertFunction("iterator_sum_float", retTy, i8_ptr_ty);
            break;
        case fesql::type::kDouble:
            callee =
                m->getOrInsertFunction("iterator_sum_double", retTy, i8_ptr_ty);
            break;
        case fesql::type::kTimestamp:
            callee = m->getOrInsertFunction("iterator_sum_timestamp", retTy,
                                            i8_ptr_ty);
            break;
        case fesql::type::kVarchar:
            callee =
                m->getOrInsertFunction("iterator_sum_string", retTy, i8_ptr_ty);
            break;
        default: {
            FAIL();
        }
    }
    ::llvm::Value* ret_val =
        builder.CreateCall(callee, ::llvm::ArrayRef<Value*>(i8_ptr));

    ::llvm::Value* ret_delete = nullptr;
    list_builder.BuildIteratorDelete(iterator, &ret_delete, status);
    builder.CreateRet(ret_val);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::llvm::StringRef symbol1("iterator_sum_i16");
    ::llvm::StringRef symbol2("iterator_sum_i32");
    ::llvm::StringRef symbol3("iterator_sum_i64");
    ::llvm::StringRef symbol4("iterator_sum_float");
    ::llvm::StringRef symbol5("iterator_sum_double");
    ::llvm::StringRef symbol6("iterator_sum_timestamp");
    //    ::llvm::StringRef symbol6("iterator_sum_string");
    ::llvm::orc::SymbolMap symbol_map;

    ::llvm::JITEvaluatedSymbol jit_symbol1(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&IteratorSumInt16)),
        ::llvm::JITSymbolFlags());
    ::llvm::JITEvaluatedSymbol jit_symbol2(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&IteratorSumInt32)),
        ::llvm::JITSymbolFlags());

    ::llvm::JITEvaluatedSymbol jit_symbol3(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&IteratorSumInt64)),
        ::llvm::JITSymbolFlags());
    ::llvm::JITEvaluatedSymbol jit_symbol4(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&IteratorSumFloat)),
        ::llvm::JITSymbolFlags());
    ::llvm::JITEvaluatedSymbol jit_symbol5(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&IteratorSumDouble)),
        ::llvm::JITSymbolFlags());
    ::llvm::JITEvaluatedSymbol jit_symbol6(
        ::llvm::pointerToJITTargetAddress(
            reinterpret_cast<void*>(&IteratorSumTimestamp)),
        ::llvm::JITSymbolFlags());

    //    ::llvm::JITEvaluatedSymbol jit_symbol6(
    //        ::llvm::pointerToJITTargetAddress(
    //            reinterpret_cast<void*>(&PrintListString)),
    //        ::llvm::JITSymbolFlags());

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
    ::fesql::udf::InitUDFSymbol(jd, mi);
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    T(*decode)
    (int8_t*) = reinterpret_cast<T (*)(int8_t*)>(load_fn_jit.getAddress());
    T res = decode(window);
    ASSERT_EQ(res, expected);
}

template <class T>
void GetListIteratorNext(T* result, const type::TableDef& table,
                         const ::fesql::type::Type& type,
                         const std::string& col, int8_t* window) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_iterator_next", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
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
            retTy = Type::getInt8PtrTy(*ctx);
            break;
        case ::fesql::type::kTimestamp: {
            ASSERT_TRUE(
                codegen::GetLLVMType(m.get(), fesql::node::kTimestamp, &retTy));
            break;
        }
        default:
            LOG(WARNING) << "invalid test type";
            FAIL();
    }
    Function* fn = Function::Create(
        FunctionType::get(Type::getVoidTy(*ctx),
                          {Type::getInt8PtrTy(*ctx), retTy->getPointerTo()},
                          false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    ScopeVar sv;
    sv.Enter("enter row scope");

    MemoryWindowDecodeIRBuilder buf_builder(table.columns(), entry_block);
    ListIRBuilder list_builder(entry_block, &sv);

    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;
    it++;
    Argument* res_ptr = &*it;

    // build column
    ::llvm::Value* column = NULL;
    bool ok = buf_builder.BuildGetCol(col, arg0, &column);
    ASSERT_TRUE(ok);

    ::llvm::Value* iter = nullptr;
    base::Status status;
    ASSERT_TRUE(list_builder.BuildIterator(column, &iter, status));
    ::llvm::Value* next1;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next1, status));
    ::llvm::Value* next2;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next2, status));

    ArithmeticIRBuilder arithmetic_ir_builder(builder.GetInsertBlock());
    ::llvm::Value* res = nullptr;
    ASSERT_TRUE(arithmetic_ir_builder.BuildAddExpr(next1, next2, &res, status));
    ::llvm::Value* next3;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next3, status));
    ASSERT_TRUE(arithmetic_ir_builder.BuildAddExpr(res, next3, &res, status));

    ::llvm::Value* next4;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next4, status));
    ASSERT_TRUE(arithmetic_ir_builder.BuildAddExpr(res, next4, &res, status));

    ::llvm::Value* next5;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next5, status));
    ASSERT_TRUE(arithmetic_ir_builder.BuildAddExpr(res, next5, &res, status));

    ::llvm::Value* ret_delete = nullptr;
    list_builder.BuildIteratorDelete(iter, &ret_delete, status);
    switch (type) {
        case type::kTimestamp: {
            codegen::TimestampIRBuilder timestamp_builder;
            ::llvm::Value* ts_output;
            ASSERT_TRUE(timestamp_builder.GetTs(builder.GetInsertBlock(), res,
                                                &ts_output));
            ASSERT_TRUE(timestamp_builder.SetTs(builder.GetInsertBlock(),
                                                res_ptr, ts_output));
            break;
        }
        default: {
            builder.CreateStore(res, res_ptr);
        }
    }
    builder.CreateRetVoid();

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::udf::InitUDFSymbol(jd, mi);
    // add codec
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    void (*decode)(int8_t*, T*) =
        reinterpret_cast<void (*)(int8_t*, T*)>(load_fn_jit.getAddress());
    decode(window, result);
}

void GetTimestampListIteratorNext(codec::Timestamp* expected,
                                  const type::TableDef& table,
                                  const ::fesql::type::Type& type,
                                  const std::string& col, int8_t* window) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_iterator_next", *ctx);
    ::fesql::udf::RegisterUDFToModule(m.get());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
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
            retTy = Type::getInt8PtrTy(*ctx);
            break;
        case ::fesql::type::kTimestamp: {
            ASSERT_TRUE(
                codegen::GetLLVMType(m.get(), fesql::node::kTimestamp, &retTy));
            break;
        }
        default:
            LOG(WARNING) << "invalid test type";
            FAIL();
    }
    Function* fn = Function::Create(
        FunctionType::get(retTy, {Type::getInt8PtrTy(*ctx)}, false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    ScopeVar sv;
    sv.Enter("enter row scope");

    MemoryWindowDecodeIRBuilder buf_builder(table.columns(), entry_block);
    ListIRBuilder list_builder(entry_block, &sv);

    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;

    // build column
    ::llvm::Value* column = NULL;
    bool ok = buf_builder.BuildGetCol(col, arg0, &column);
    ASSERT_TRUE(ok);

    ::llvm::Value* iter = nullptr;
    base::Status status;
    ASSERT_TRUE(list_builder.BuildIterator(column, &iter, status));
    ::llvm::Value* next1;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next1, status));
    ::llvm::Value* next2;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next2, status));

    ArithmeticIRBuilder arithmetic_ir_builder(builder.GetInsertBlock());
    ::llvm::Value* res = nullptr;
    ASSERT_TRUE(arithmetic_ir_builder.BuildAddExpr(next1, next2, &res, status));
    ::llvm::Value* next3;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next3, status));
    ASSERT_TRUE(arithmetic_ir_builder.BuildAddExpr(res, next3, &res, status));

    ::llvm::Value* next4;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next4, status));
    ASSERT_TRUE(arithmetic_ir_builder.BuildAddExpr(res, next4, &res, status));

    ::llvm::Value* next5;
    ASSERT_TRUE(list_builder.BuildIteratorNext(iter, &next5, status));
    ASSERT_TRUE(arithmetic_ir_builder.BuildAddExpr(res, next5, &res, status));

    ::llvm::Value* ret_delete = nullptr;
    list_builder.BuildIteratorDelete(iter, &ret_delete, status);
    builder.CreateRet(res);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::udf::InitUDFSymbol(jd, mi);
    // add codec
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    codec::Timestamp* (*decode)(int8_t*) =
        reinterpret_cast<codec::Timestamp* (*)(int8_t*)>(
            load_fn_jit.getAddress());
    codec::Timestamp* result = decode(window);
    ASSERT_EQ(result->ts_, expected->ts_);
}
template <class T>
void RunListAtCase(T expected, const type::TableDef& table,
                   const ::fesql::type::Type& type, const std::string& col,
                   int8_t* window, int32_t pos) {
    T result;
    GetListAtPos<T>(table, &result, type, col, window, pos);
    ASSERT_EQ(result, expected);
}

void RunTimestampListAtCase(codec::Timestamp* expected,
                            const type::TableDef& table,
                            const ::fesql::type::Type& type,
                            const std::string& col, int8_t* window,
                            int32_t pos) {
    codec::Timestamp result;
    GetListAtPos<codec::Timestamp>(table, &result, type, col, window, pos);
    ASSERT_EQ(result.ts_, expected->ts_);
}

template <class V>
void RunListIteratorCase(V expected, const type::TableDef& table,
                         const ::fesql::type::Type& type,
                         const std::string& col, int8_t* window) {
    GetListIterator(expected, table, type, col, window);
}

template <class V>
void RunListIteratorNextCase(V expected, const type::TableDef& table,
                             const ::fesql::type::Type& type,
                             const std::string& col, int8_t* window) {
    V result;
    GetListIteratorNext(&result, table, type, col, window);
    ASSERT_EQ(result, expected);
}

void RunTimestampListIteratorNextCase(const codec::Timestamp& expected,
                                      const type::TableDef& table,
                                      const ::fesql::type::Type& type,
                                      const std::string& col, int8_t* window) {
    codec::Timestamp result;
    GetListIteratorNext(&result, table, type, col, window);
    ASSERT_EQ(result.ts_, expected.ts_);
}
TEST_F(ListIRBuilderTest, list_int16_at_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListAtCase<int16_t>(2, table, ::fesql::type::kInt16, "col2", ptr, 0);
    RunListAtCase<int16_t>(22, table, ::fesql::type::kInt16, "col2", ptr, 1);
    RunListAtCase<int16_t>(22222, table, ::fesql::type::kInt16, "col2", ptr, 4);
    RunListAtCase<int16_t>(2222, table, ::fesql::type::kInt16, "col2", ptr, 3);
    RunListAtCase<int16_t>(222, table, ::fesql::type::kInt16, "col2", ptr, 2);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int32_at_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListAtCase<int32_t>(1, table, ::fesql::type::kInt32, "col1", ptr, 0);
    RunListAtCase<int32_t>(11, table, ::fesql::type::kInt32, "col1", ptr, 1);
    RunListAtCase<int32_t>(11111, table, ::fesql::type::kInt32, "col1", ptr, 4);
    RunListAtCase<int32_t>(1111, table, ::fesql::type::kInt32, "col1", ptr, 3);
    RunListAtCase<int32_t>(111, table, ::fesql::type::kInt32, "col1", ptr, 2);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int64_at_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListAtCase<int64_t>(5, table, ::fesql::type::kInt32, "col5", ptr, 0);
    RunListAtCase<int64_t>(55, table, ::fesql::type::kInt32, "col5", ptr, 1);
    RunListAtCase<int64_t>(55555, table, ::fesql::type::kInt32, "col5", ptr, 4);
    RunListAtCase<int64_t>(5555, table, ::fesql::type::kInt32, "col5", ptr, 3);
    RunListAtCase<int64_t>(555, table, ::fesql::type::kInt32, "col5", ptr, 2);
    free(ptr);
}
TEST_F(ListIRBuilderTest, list_float_at_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListAtCase<float>(3.1f, table, ::fesql::type::kFloat, "col3", ptr, 0);
    RunListAtCase<float>(33.1f, table, ::fesql::type::kFloat, "col3", ptr, 1);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_double_at_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListAtCase<double>(4.1, table, ::fesql::type::kDouble, "col4", ptr, 0);
    RunListAtCase<double>(44.1, table, ::fesql::type::kDouble, "col4", ptr, 1);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_timestamp_at_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    codec::Timestamp ts(1590115420000L);
    RunTimestampListAtCase(&ts, table, ::fesql::type::kTimestamp, "std_ts", ptr,
                           0);
    free(ptr);
}

// TODO(chenjing): support list string at operation
// TEST_F(ListIRBuilderTest, list_string_at_test) {
//    int8_t* ptr = NULL;
//    std::vector<Row> rows;
//    BuildWindow(rows, &ptr);
//    std::string str("1");
//
//    RunListStringAtCase(str, ::fesql::type::kVarchar, "col6", ptr, 0);
//    free(ptr);
//}

TEST_F(ListIRBuilderTest, list_int32_iterator_sum_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorCase<int32_t>(1 + 11 + 111 + 1111 + 11111, table,
                                 ::fesql::type::kInt32, "col1", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int16_iterator_sum_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorCase<int16_t>(2 + 22 + 222 + 2222 + 22222, table,
                                 ::fesql::type::kInt16, "col2", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int64_iterator_sum_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorCase<int64_t>(5L + 55L + 555L + 5555L + 55555L, table,
                                 ::fesql::type::kInt64, "col5", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_float_iterator_sum_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorCase<float>(3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f,
                               table, ::fesql::type::kFloat, "col3", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_double_iterator_sum_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorCase<double>(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1, table,
                                ::fesql::type::kDouble, "col4", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_timestamp_iterator_sum_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    codec::Timestamp ts(1590115420000L + 1590115430000 + 1590115440000 +
                        1590115450000 + 1590115460000);
    RunTimestampListIteratorNextCase(ts, table, ::fesql::type::kTimestamp,
                                     "std_ts", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int32_iterator_next_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorNextCase<int32_t>(1 + 11 + 111 + 1111 + 11111, table,
                                     ::fesql::type::kInt32, "col1", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int16_iterator_next_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorNextCase<int16_t>(2 + 22 + 222 + 2222 + 22222, table,
                                     ::fesql::type::kInt16, "col2", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int64_iterator_next_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorNextCase<int64_t>(5L + 55L + 555L + 5555L + 55555L, table,
                                     ::fesql::type::kInt64, "col5", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_float_iterator_next_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorNextCase<float>(3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f,
                                   table, ::fesql::type::kFloat, "col3", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_double_iterator_next_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    RunListIteratorNextCase<double>(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1,
                                    table, ::fesql::type::kDouble, "col4", ptr);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_timestamp_iterator_next_test) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);
    codec::Timestamp ts(1590115420000L + 1590115430000 + 1590115440000 +
                        1590115450000 + 1590115460000);
    RunTimestampListIteratorNextCase(ts, table, ::fesql::type::kTimestamp,
                                     "std_ts", ptr);
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
