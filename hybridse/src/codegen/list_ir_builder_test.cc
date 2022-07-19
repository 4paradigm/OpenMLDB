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

#include "codegen/list_ir_builder.h"
#include <stdio.h>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/buf_ir_builder.h"
#include "codegen/codegen_base_test.h"
#include "codegen/ir_base_builder.h"
#include "codegen/ir_base_builder_test.h"
#include "codegen/string_ir_builder.h"
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
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "vm/sql_compiler.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

using hybridse::base::ConstIterator;
using hybridse::sqlcase::SqlCase;
using hybridse::common::kCodegenError;

struct TestString {
    int32_t size;
    char* data = nullptr;
};

void AssertStrEq(std::string exp, int8_t* ptr) {
    openmldb::base::StringRef* ts = reinterpret_cast<openmldb::base::StringRef*>(ptr);
    ASSERT_EQ(exp.size(), ts->size_);
    std::string str(ts->data_, ts->size_);
    ASSERT_EQ(str, exp);
}

namespace hybridse {
namespace codegen {

using hybridse::codec::ListRef;
using hybridse::codec::ListV;
using hybridse::codec::Row;

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
    ::hybridse::codec::IteratorRef* iter_ref =
        reinterpret_cast<::hybridse::codec::IteratorRef*>(input);
    ConstIterator<uint64_t, V>* iter =
        reinterpret_cast<ConstIterator<uint64_t, V>*>(iter_ref->iterator);
    while (iter->Valid()) {
        result += iter->GetValue();
        iter->Next();
    }
    return result;
}

using ListOfRow = ListRef<udf::LiteralTypedRow<>>;

template <class T>
void RunListIteratorCase(T expected, const type::TableDef& table,
                         const std::string& col, const ListOfRow& window) {
    auto sum_func =
        ModuleFunctionBuilder().args<ListOfRow>().returns<T>().build(
            [&](codegen::CodeGenContext* ctx) {
                BasicBlock* entry_block = ctx->GetCurrentBlock();
                ScopeVar sv;

                vm::SchemasContext schemas_context;
                schemas_context.BuildTrivial(table.catalog(), {&table});
                MemoryWindowDecodeIRBuilder buf_builder(&schemas_context,
                                                        entry_block);

                ListIRBuilder list_builder(entry_block, &sv);

                auto fn = ctx->GetCurrentFunction();
                IRBuilder<> builder(entry_block);
                Function::arg_iterator it = fn->arg_begin();
                Argument* arg0 = &*it;

                // build column
                size_t schema_idx;
                size_t col_idx;
                schemas_context.ResolveColumnIndexByName("", "", col, &schema_idx,
                                                         &col_idx);

                ::llvm::Value* column = NULL;
                ::llvm::Value* list_ptr = builder.CreatePointerCast(
                    arg0, builder.getInt8Ty()->getPointerTo());
                CHECK_TRUE(buf_builder.BuildGetCol(schema_idx, col_idx,
                                                   list_ptr, &column),
                           kCodegenError);

                ::llvm::Value* iterator = nullptr;
                node::NodeManager* nm = ctx->node_manager();
                auto elem_type = DataTypeTrait<T>::to_type_node(nm);
                CHECK_STATUS(
                    list_builder.BuildIterator(column, elem_type, &iterator));
                ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
                ::llvm::Value* i8_ptr =
                    builder.CreatePointerCast(iterator, i8_ptr_ty);

                ::llvm::Value* ret_addr = fn->arg_begin() + fn->arg_size() - 1;
                llvm::FunctionCallee callee =
                    ctx->GetModule()->getOrInsertFunction(
                        "iterator_sum_" + elem_type->GetName(),
                        reinterpret_cast<::llvm::PointerType*>(
                            ret_addr->getType())
                            ->getElementType(),
                        i8_ptr_ty);

                ::llvm::Value* ret_val = builder.CreateCall(
                    callee, ::llvm::ArrayRef<Value*>(i8_ptr));

                ::llvm::Value* ret_delete = nullptr;
                list_builder.BuildIteratorDelete(iterator, elem_type,
                                                 &ret_delete);
                builder.CreateStore(ret_val, ret_addr);
                builder.CreateRetVoid();
                return Status::OK();
            });

    ASSERT_TRUE(sum_func.valid());
    T res = sum_func(window);
    ASSERT_FLOAT_EQ(res, expected);
}

template <class T>
void RunListIteratorByRowCase(T expected, const type::TableDef& table,
                              const std::string& col, const ListOfRow& window) {
    const int32_t col_offset = 7 + 4 + 2 + 4;
    auto sum_func =
        ModuleFunctionBuilder().args<ListOfRow>().returns<T>().build(
            [&](codegen::CodeGenContext* ctx) {
                BasicBlock* entry_block = ctx->GetCurrentBlock();
                ScopeVar sv;

                ListIRBuilder list_builder(entry_block, &sv);
                auto fn = ctx->GetCurrentFunction();
                IRBuilder<> builder(entry_block);
                Function::arg_iterator it = fn->arg_begin();
                Argument* arg0 = &*it;

                ::llvm::Value* iterator = nullptr;
                node::NodeManager* nm = ctx->node_manager();
                auto row_type =
                    DataTypeTrait<udf::LiteralTypedRow<>>::to_type_node(nm);

                auto list_ref_ptr = arg0;

                CHECK_STATUS(list_builder.BuildIterator(list_ref_ptr, row_type,
                                                        &iterator));
                ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
                ::llvm::Value* i8_ptr =
                    builder.CreatePointerCast(iterator, i8_ptr_ty);

                auto col_offset_val = builder.getInt32(col_offset);
                ::llvm::Value* ret_addr = fn->arg_begin() + fn->arg_size() - 1;
                llvm::FunctionCallee callee =
                    ctx->GetModule()->getOrInsertFunction(
                        "iterator_sum_" + row_type->GetName(),
                        reinterpret_cast<::llvm::PointerType*>(
                            ret_addr->getType())
                            ->getElementType(),
                        i8_ptr_ty, col_offset_val->getType());

                ::llvm::Value* ret_val =
                    builder.CreateCall(callee, {i8_ptr, col_offset_val});

                ::llvm::Value* ret_delete = nullptr;
                list_builder.BuildIteratorDelete(iterator, row_type,
                                                 &ret_delete);

                builder.CreateStore(ret_val, ret_addr);
                builder.CreateRetVoid();
                return Status::OK();
            });

    ASSERT_TRUE(sum_func.valid());
    T res = sum_func(window);
    ASSERT_FLOAT_EQ(res, expected);
}

template <class T>
void RunInnerListIteratorCase(T expected, const type::TableDef& table,
                              const std::string& col, const ListOfRow& window,
                              int64_t start_offset, int64_t end_offset,
                              bool inner_range) {
    auto sum_func =
        ModuleFunctionBuilder()
            .args<ListRef<udf::LiteralTypedRow<>>>()
            .returns<T>()
            .build([&](codegen::CodeGenContext* ctx) {
                BasicBlock* entry_block = ctx->GetCurrentBlock();
                ScopeVar sv;

                vm::SchemasContext schemas_context;
                schemas_context.BuildTrivial(table.catalog(), {&table});
                MemoryWindowDecodeIRBuilder buf_builder(&schemas_context,
                                                        entry_block);
                size_t schema_idx;
                size_t col_idx;
                schemas_context.ResolveColumnIndexByName("", "", col, &schema_idx,
                                                         &col_idx);

                ListIRBuilder list_builder(entry_block, &sv);

                IRBuilder<> builder(entry_block);
                auto fn = ctx->GetCurrentFunction();
                Function::arg_iterator it = fn->arg_begin();
                Argument* arg0 = &*it;

                ::llvm::Value* inner_list = NULL;
                ::llvm::Value* list_gep_0 = builder.CreateStructGEP(
                    cast<PointerType>(arg0->getType()->getScalarType())
                        ->getElementType(),
                    arg0, 0);
                auto list = reinterpret_cast<vm::Window*>(window.list);

                ::llvm::Value* row_key =
                    list->GetCount() > 0
                        ? builder.getInt64(list->GetFrontRow().first)
                        : builder.getInt64(0);
                ::llvm::Value* list_ptr = builder.CreateLoad(list_gep_0);
                if (inner_range) {
                    CHECK_TRUE(buf_builder.BuildInnerRangeList(
                                   list_ptr, row_key, start_offset, end_offset,
                                   &inner_list),
                               kCodegenError);
                } else {
                    CHECK_TRUE(
                        buf_builder.BuildInnerRowsList(list_ptr, start_offset,
                                                       end_offset, &inner_list),
                        kCodegenError);
                }
                // build column
                ::llvm::Value* column = NULL;
                ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
                ::llvm::Type* list_ref_ty =
                    reinterpret_cast<::llvm::PointerType*>(arg0->getType())
                        ->getElementType();
                ::llvm::Value* inner_list_ref = CreateAllocaAtHead(
                    &builder, list_ref_ty, "list_ref_alloca");
                ::llvm::Value* list_ref_gep_0 = builder.CreateStructGEP(
                    cast<PointerType>(arg0->getType()->getScalarType())
                        ->getElementType(),
                    inner_list_ref, 0);
                builder.CreateStore(inner_list, list_ref_gep_0);
                inner_list_ref =
                    builder.CreatePointerCast(inner_list_ref, i8_ptr_ty);
                CHECK_TRUE(buf_builder.BuildGetCol(schema_idx, col_idx,
                                                   inner_list_ref, &column),
                           kCodegenError);

                ::llvm::Value* iterator = nullptr;
                node::NodeManager* nm = ctx->node_manager();
                auto elem_type = DataTypeTrait<T>::to_type_node(nm);
                CHECK_STATUS(
                    list_builder.BuildIterator(column, elem_type, &iterator));

                ::llvm::Value* i8_ptr =
                    builder.CreatePointerCast(iterator, i8_ptr_ty);
                ::llvm::Value* ret_addr = fn->arg_begin() + fn->arg_size() - 1;
                llvm::FunctionCallee callee =
                    ctx->GetModule()->getOrInsertFunction(
                        "iterator_sum_" + elem_type->GetName(),
                        reinterpret_cast<::llvm::PointerType*>(
                            ret_addr->getType())
                            ->getElementType(),
                        i8_ptr_ty);

                ::llvm::Value* ret_val = builder.CreateCall(
                    callee, ::llvm::ArrayRef<Value*>(i8_ptr));

                ::llvm::Value* ret_delete = nullptr;
                list_builder.BuildIteratorDelete(iterator, elem_type,
                                                 &ret_delete);
                builder.CreateStore(ret_val, ret_addr);
                builder.CreateRetVoid();
                return Status::OK();
            });

    ASSERT_TRUE(sum_func.valid());
    T res = sum_func(window);
    ASSERT_FLOAT_EQ(res, expected);
}

template <class V>
void RunInnerRangeListIteratorCase(V expected, const type::TableDef& table,
                                   const std::string& col,
                                   const ListOfRow& window, int64_t start,
                                   int64_t end) {
    RunInnerListIteratorCase(expected, table, col, window, start, end, true);
}

template <class V>
void RunInnerRowsListIteratorCase(V expected, const type::TableDef& table,
                                  const std::string& col,
                                  const ListOfRow& window, int64_t start,
                                  int64_t end) {
    RunInnerListIteratorCase(expected, table, col, window, start, end, false);
}

template <class T>
void RunListIteratorSumCase(T expected, const type::TableDef& table,
                            const std::string& col, const ListOfRow& window) {
    auto func =
        ModuleFunctionBuilder()
            .args<ListRef<udf::LiteralTypedRow<>>>()
            .returns<T>()
            .build([&](codegen::CodeGenContext* ctx) {
                BasicBlock* entry_block = ctx->GetCurrentBlock();
                ScopeVar sv;

                vm::SchemasContext schemas_context;
                schemas_context.BuildTrivial(table.catalog(), {&table});
                MemoryWindowDecodeIRBuilder buf_builder(&schemas_context,
                                                        entry_block);
                size_t schema_idx;
                size_t col_idx;
                schemas_context.ResolveColumnIndexByName("", "", col, &schema_idx,
                                                         &col_idx);

                ListIRBuilder list_builder(entry_block, &sv);

                IRBuilder<> builder(entry_block);
                auto fn = ctx->GetCurrentFunction();
                Function::arg_iterator it = fn->arg_begin();
                Argument* arg0 = &*it;

                node::NodeManager* nm = ctx->node_manager();
                auto elem_type = DataTypeTrait<T>::to_type_node(nm);

                // build column
                ::llvm::Value* column = NULL;
                ::llvm::Value* list_ptr = builder.CreatePointerCast(
                    arg0, builder.getInt8Ty()->getPointerTo());
                CHECK_TRUE(buf_builder.BuildGetCol(schema_idx, col_idx,
                                                   list_ptr, &column),
                           kCodegenError);

                ::llvm::Value* iter = nullptr;
                CHECK_STATUS(
                    list_builder.BuildIterator(column, elem_type, &iter));
                NativeValue next1_wrapper;
                CHECK_STATUS(list_builder.BuildIteratorNext(
                    iter, elem_type, false, &next1_wrapper));
                NativeValue next2_wrapper;
                CHECK_STATUS(list_builder.BuildIteratorNext(
                    iter, elem_type, false, &next2_wrapper));

                ArithmeticIRBuilder arithmetic_ir_builder(
                    builder.GetInsertBlock());
                NativeValue res;
                base::Status status;
                CHECK_STATUS(arithmetic_ir_builder.BuildAddExpr(
                    next1_wrapper, next2_wrapper, &res));

                NativeValue next3_wrapper;
                CHECK_STATUS(list_builder.BuildIteratorNext(
                    iter, elem_type, false, &next3_wrapper));
                CHECK_STATUS(arithmetic_ir_builder.BuildAddExpr(
                    res, next3_wrapper, &res));

                NativeValue next4_wrapper;
                CHECK_STATUS(list_builder.BuildIteratorNext(
                    iter, elem_type, false, &next4_wrapper));
                CHECK_STATUS(arithmetic_ir_builder.BuildAddExpr(
                    res, next4_wrapper, &res));

                NativeValue next5_wrapper;
                CHECK_STATUS(list_builder.BuildIteratorNext(
                    iter, elem_type, false, &next5_wrapper));
                CHECK_STATUS(arithmetic_ir_builder.BuildAddExpr(
                    res, next5_wrapper, &res));

                ::llvm::Value* ret_delete = nullptr;
                list_builder.BuildIteratorDelete(iter, elem_type, &ret_delete);
                ::llvm::Value* raw_res = res.GetRaw();
                if (codegen::TypeIRBuilder::IsStructPtr(res.GetType())) {
                    raw_res = builder.CreateLoad(res.GetRaw());
                }
                builder.CreateStore(raw_res,
                                    fn->arg_begin() + fn->arg_size() - 1);
                builder.CreateRetVoid();
                return Status::OK();
            });

    T result = func(window);
    ASSERT_EQ(expected, result);
}

template <class T>
void RunListIteratorNextCase(T expected, const type::TableDef& table,
                             const std::string& col, const ListOfRow& window) {
    auto func =
        ModuleFunctionBuilder()
            .args<ListRef<udf::LiteralTypedRow<>>>()
            .returns<T>()
            .build([&](codegen::CodeGenContext* ctx) {
                BasicBlock* entry_block = ctx->GetCurrentBlock();
                ScopeVar sv;

                vm::SchemasContext schemas_context;
                schemas_context.BuildTrivial(table.catalog(), {&table});
                MemoryWindowDecodeIRBuilder buf_builder(&schemas_context,
                                                        entry_block);
                size_t schema_idx;
                size_t col_idx;
                schemas_context.ResolveColumnIndexByName("", "", col, &schema_idx,
                                                         &col_idx);

                ListIRBuilder list_builder(entry_block, &sv);

                auto fn = ctx->GetCurrentFunction();
                IRBuilder<> builder(entry_block);
                Function::arg_iterator it = fn->arg_begin();
                Argument* arg0 = &*it;

                // build column
                ::llvm::Value* column = NULL;
                ::llvm::Value* list_ptr = builder.CreatePointerCast(
                    arg0, builder.getInt8Ty()->getPointerTo());
                CHECK_TRUE(buf_builder.BuildGetCol(schema_idx, col_idx,
                                                   list_ptr, &column),
                           kCodegenError);

                node::NodeManager* nm = ctx->node_manager();
                auto elem_type = DataTypeTrait<T>::to_type_node(nm);
                ::llvm::Value* iter = nullptr;
                base::Status status;
                CHECK_STATUS(
                    list_builder.BuildIterator(column, elem_type, &iter));
                NativeValue next1_wrapper;
                CHECK_STATUS(list_builder.BuildIteratorNext(
                    iter, elem_type, false, &next1_wrapper));
                ::llvm::Value* next1 = next1_wrapper.GetValue(&builder);
                ::llvm::Value* ret_delete = nullptr;
                list_builder.BuildIteratorDelete(iter, elem_type, &ret_delete);
                switch (elem_type->base_) {
                    case node::kVarchar: {
                        auto res_ptr = fn->arg_begin() + 1;
                        codegen::StringIRBuilder string_builder(
                            ctx->GetModule());
                        CHECK_TRUE(
                            string_builder.CopyFrom(builder.GetInsertBlock(),
                                                    next1, res_ptr),
                            kCodegenError);
                        builder.CreateRetVoid();
                        break;
                    }
                    default: {
                        if (codegen::TypeIRBuilder::IsStructPtr(
                                next1->getType())) {
                            next1 = builder.CreateLoad(next1);
                        }
                        builder.CreateStore(
                            next1, fn->arg_begin() + fn->arg_size() - 1);
                        builder.CreateRetVoid();
                    }
                }
                return Status::OK();
            });

    T result = func(window);
    ASSERT_EQ(expected, result);
}

TEST_F(ListIRBuilderTest, ListInt32IteratorSumTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorSumCase<int32_t>(1 + 11 + 111 + 1111 + 11111, table, "col1",
                                    window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListInt16IteratorSumTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorSumCase<int16_t>(2 + 22 + 222 + 2222 + 22222, table, "col2",
                                    window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListInt64IteratorSumTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorSumCase<int64_t>(5L + 55L + 555L + 5555L + 55555L, table,
                                    "col5", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListFloatIteratorSumTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorSumCase<float>(3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f,
                                  table, "col3", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListDoubleIteratorSumTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorCase<double>(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1, table,
                                "col4", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListIteratorByRowDoubleSumTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorByRowCase<double>(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1,
                                     table, "col4", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListTimestampIteratorSumTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    openmldb::base::Timestamp ts(1590115420000L + 1590115430000 + 1590115440000 +
                        1590115450000 + 1590115460000);
    RunListIteratorSumCase(ts, table, "std_ts", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListInt32IteratorNextTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorNextCase<int32_t>(1, table, "col1", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListInt16IteratorNextTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorNextCase<int16_t>(2, table, "col2", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListInt64IteratorNextTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorNextCase<int64_t>(5L, table, "col5", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListFloatIteratorNextTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorNextCase<float>(3.1f, table, "col3", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListDoubleIteratorNextTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    RunListIteratorNextCase<double>(4.1, table, "col4", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListTimestampIteratorNextTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    openmldb::base::Timestamp ts(1590115420000L);
    RunListIteratorNextCase(ts, table, "std_ts", window);
    free(window.list);
}
TEST_F(ListIRBuilderTest, ListStringIteratorNextTest) {
    ListOfRow window;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &window.list);
    openmldb::base::StringRef str(strlen("1"), strdup("1"));
    RunListIteratorNextCase(str, table, "col6", window);
    free(window.list);
}

TEST_F(ListIRBuilderTest, ListDoubleInnerRangeTest) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);

    ListOfRow row_list;
    vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -3600000, 0);
    codec::RowView row_view(table.columns());
    for (auto row : rows) {
        row_view.Reset(row.buf());
        window.BufferData(row_view.GetTimestampUnsafe(6), row);
    }
    row_list.list = reinterpret_cast<int8_t*>(&window);

    RunInnerRangeListIteratorCase<double>(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1,
                                          table, "col4", row_list, 0, -3600000);

    RunInnerRangeListIteratorCase<double>(44444.1, table, "col4", row_list, 0,
                                          -5000);
    RunInnerRangeListIteratorCase<double>(4444.1 + 44444.1, table, "col4",
                                          row_list, 0, -10000);
    RunInnerRangeListIteratorCase<double>(444.1 + 4444.1 + 44444.1, table,
                                          "col4", row_list, 0, -20000);

    RunInnerRangeListIteratorCase<double>(444.1 + 4444.1, table, "col4",
                                          row_list, -10000, -20000);
    free(ptr);
}

TEST_F(ListIRBuilderTest, ListDoubleInnerRowsTest) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindow(table, rows, &ptr);

    ListOfRow row_list;
    vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -3600000, 0);
    codec::RowView row_view(table.columns());
    for (auto row : rows) {
        row_view.Reset(row.buf());
        window.BufferData(row_view.GetTimestampUnsafe(6), row);
    }
    row_list.list = reinterpret_cast<int8_t*>(&window);

    RunInnerRowsListIteratorCase<double>(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1,
                                         table, "col4", row_list, 0, 100);

    RunInnerRowsListIteratorCase<double>(44444.1, table, "col4", row_list, 0,
                                         0);
    RunInnerRowsListIteratorCase<double>(4444.1 + 44444.1, table, "col4",
                                         row_list, 0, 1);
    RunInnerRowsListIteratorCase<double>(444.1 + 4444.1 + 44444.1, table,
                                         "col4", row_list, 0, 2);

    RunInnerRowsListIteratorCase<double>(444.1 + 4444.1, table, "col4",
                                         row_list, 1, 2);
    free(ptr);
}

}  // namespace codegen
}  // namespace hybridse

extern "C" {
int16_t iterator_sum_int16(int8_t* input) {
    return hybridse::codegen::IteratorSum<int16_t>(input);
}
int32_t iterator_sum_int32(int8_t* input) {
    return hybridse::codegen::IteratorSum<int32_t>(input);
}
int64_t iterator_sum_int64(int8_t* input) {
    return hybridse::codegen::IteratorSum<int64_t>(input);
}
float iterator_sum_float(int8_t* input) {
    return hybridse::codegen::IteratorSum<float>(input);
}
double iterator_sum_double(int8_t* input) {
    return hybridse::codegen::IteratorSum<double>(input);
}
double iterator_sum_row(int8_t* input, int32_t offset) {
    double result = 0;
    ::hybridse::codec::IteratorRef* iter_ref =
        (::hybridse::codec::IteratorRef*)(input);
    auto iter =
        (ConstIterator<uint64_t, hybridse::codec::Row>*)(iter_ref->iterator);
    while (iter->Valid()) {
        auto& row = iter->GetValue();
        auto buf = row.buf(0);
        result += *reinterpret_cast<double*>(buf + offset);
        iter->Next();
    }
    return result;
}
}  // extern "C"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
