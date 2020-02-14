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
#include <udf/udf.h>
#include <cstdlib>
#include <memory>
#include <vector>
#include "codegen/buf_ir_builder.h"
#include "codegen/codegen_base_test.h"
#include "codegen/ir_base_builder.h"
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
#include "storage/codec.h"
#include "storage/type_ir_builder.h"
#include "storage/window.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

struct TestString {
    int32_t size;
    char* data;
};

void AssertStrEq(std::string exp, int8_t* ptr) {
    ::fesql::storage::StringRef* ts =
        reinterpret_cast<::fesql::storage::StringRef*>(ptr);
    ASSERT_EQ(exp.size(), ts->size);
    std::string str(ts->data, ts->size);
    ASSERT_EQ(str, exp);
}

namespace fesql {
namespace codegen {

class ListIRBuilderTest : public ::testing::Test {
 public:
    ListIRBuilderTest() {}
    ~ListIRBuilderTest() {}
};

template <class T>
void GetListAtPos(T* result, const ::fesql::type::Type& type,
                   const std::string& col, int8_t* window, int32_t pos) {
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
        default:
            LOG(WARNING) << "invalid test type";
            FAIL();
    }
    Function* fn = Function::Create(
        FunctionType::get(
            retTy, {Type::getInt8PtrTy(*ctx), Type::getInt32Ty(*ctx)}, false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    ScopeVar sv;
    sv.Enter("enter row scope");

    BufNativeIRBuilder buf_builder(&table, entry_block, &sv);
    ListIRBuilder list_builder(entry_block, &sv);

    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;

    // build column
    ::llvm::Value* column = NULL;
    bool ok = buf_builder.BuildGetCol(col, arg0, &column);
    ASSERT_TRUE(ok);

    ::llvm::Value* val;
    base::Status status;
    ASSERT_TRUE(
        list_builder.BuildAt(column, builder.getInt32(pos), &val, status));
    builder.CreateRet(val);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    auto& jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());

    ::fesql::udf::InitUDFSymbol(jd, mi);
    // add codec
    ::fesql::storage::InitCodecSymbol(jd, mi);
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    T(*decode)
        (int8_t*, int32_t) =
    reinterpret_cast<T (*)(int8_t*, int32_t)>(load_fn_jit.getAddress());
    *result = decode(window, pos);
}

template <class T>
void RunListAtCase(T expected, const ::fesql::type::Type& type,
                   const std::string& col, int8_t* window, int32_t pos) {
    T result;
    GetListAtPos<T>(&result, type, col, window, pos);
    ASSERT_EQ(result, expected);

}

TEST_F(ListIRBuilderTest, list_int16_at_test) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow2(rows, &ptr);
    RunListAtCase<int16_t>(2, ::fesql::type::kInt16, "col2", ptr, 0);
    RunListAtCase<int16_t>(22, ::fesql::type::kInt16, "col2", ptr, 1);
    RunListAtCase<int16_t>(22222, ::fesql::type::kInt16, "col2", ptr, 4);
    RunListAtCase<int16_t>(2222, ::fesql::type::kInt16, "col2", ptr, 3);
    RunListAtCase<int16_t>(222, ::fesql::type::kInt16, "col2", ptr, 2);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int32_at_test) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow2(rows, &ptr);
    RunListAtCase<int32_t>(1, ::fesql::type::kInt32, "col1", ptr, 0);
    RunListAtCase<int32_t>(11, ::fesql::type::kInt32, "col1", ptr, 1);
    RunListAtCase<int32_t>(11111, ::fesql::type::kInt32, "col1", ptr, 4);
    RunListAtCase<int32_t>(1111, ::fesql::type::kInt32, "col1", ptr, 3);
    RunListAtCase<int32_t>(111, ::fesql::type::kInt32, "col1", ptr, 2);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_int64_at_test) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow2(rows, &ptr);
    RunListAtCase<int64_t >(5, ::fesql::type::kInt32, "col5", ptr, 0);
    RunListAtCase<int64_t>(55, ::fesql::type::kInt32, "col5", ptr, 1);
    RunListAtCase<int64_t>(55555, ::fesql::type::kInt32, "col5", ptr, 4);
    RunListAtCase<int64_t>(5555, ::fesql::type::kInt32, "col5", ptr, 3);
    RunListAtCase<int64_t>(555, ::fesql::type::kInt32, "col5", ptr, 2);
    free(ptr);
}
TEST_F(ListIRBuilderTest, list_float_at_test) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow2(rows, &ptr);
    RunListAtCase<int64_t>(3.1f, ::fesql::type::kFloat, "col3", ptr, 0);
    RunListAtCase<int64_t>(33.1f, ::fesql::type::kFloat, "col3", ptr, 1);
    free(ptr);
}

TEST_F(ListIRBuilderTest, list_double_at_test) {
    int8_t* ptr = NULL;
    std::vector<fesql::storage::Row> rows;
    BuildWindow2(rows, &ptr);
    RunListAtCase<double>(4.1, ::fesql::type::kDouble, "col4", ptr, 0);
    RunListAtCase<double>(44.1, ::fesql::type::kDouble, "col4", ptr, 1);
    free(ptr);
}

// TODO(chenjing): support list string at operation
//TEST_F(ListIRBuilderTest, list_string_at_test) {
//    int8_t* ptr = NULL;
//    std::vector<fesql::storage::Row> rows;
//    BuildWindow(rows, &ptr);
//    std::string str("1");
//
//    RunListStringAtCase(str, ::fesql::type::kVarchar, "col6", ptr, 0);
//    free(ptr);
//}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
