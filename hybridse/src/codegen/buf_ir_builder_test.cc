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

#include "codegen/buf_ir_builder.h"

#include <stdio.h>

#include <cstdlib>
#include <memory>
#include <vector>

#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "codec/type_codec.h"
#include "codegen/codegen_base_test.h"
#include "codegen/context.h"
#include "codegen/ir_base_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "codegen/window_ir_builder.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "vm/jit_wrapper.h"

using namespace llvm;       // NOLINT

DECLARE_bool(enable_spark_unsaferow_format);

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
    ::hybridse::codec::ListRef<>* list_ref =
        reinterpret_cast<::hybridse::codec::ListRef<>*>(input);
    ::hybridse::codec::ColumnImpl<T>* column =
        reinterpret_cast<::hybridse::codec::ColumnImpl<T>*>(list_ref->list);
    auto col = column->GetIterator();
    std::cout << "[";
    while (col->Valid()) {
        T v = col->GetValue();
        col->Next();
        std::cout << v << ",";
        sum += v;
    }
    std::cout << "]";
    return sum;
}

int16_t PrintListInt16(int8_t* input) { return PrintList<int16_t>(input); }
int32_t PrintListInt32(int8_t* input) { return PrintList<int32_t>(input); }
int64_t PrintListInt64(int8_t* input) { return PrintList<int64_t>(input); }
int64_t PrintListTimestamp(int8_t* input) { return PrintList<int64_t>(input); }
float PrintListFloat(int8_t* input) { return PrintList<float>(input); }
double PrintListDouble(int8_t* input) { return PrintList<double>(input); }
int32_t PrintListString(int8_t* input) {
    int32_t cnt = 0;
    if (nullptr == input) {
        std::cout << "list is null";
    } else {
        std::cout << "list ptr is ok" << std::endl;
    }
    ::hybridse::codec::ListRef<>* list_ref =
        reinterpret_cast<::hybridse::codec::ListRef<>*>(input);
    ::hybridse::codec::StringColumnImpl* column =
        reinterpret_cast<::hybridse::codec::StringColumnImpl*>(list_ref->list);
    auto col = column->GetIterator();
    std::cout << "[";
    while (col->Valid()) {
        openmldb::base::StringRef v = col->GetValue();
        col->Next();
        std::string str(v.data_, v.size_);
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

namespace hybridse {
namespace codegen {

using hybridse::codec::Row;
using hybridse::sqlcase::SqlCase;
class BufIRBuilderTest : public ::testing::Test {
 public:
    BufIRBuilderTest() {}
    ~BufIRBuilderTest() {}
};

void RunEncode(::hybridse::type::TableDef& table, // NOLINT
               int8_t** output_ptr) {
    SqlCase::TableInfo table_info;
    ASSERT_TRUE(SqlCase::CreateTableInfoFromYaml(
        hybridse::sqlcase::FindSqlCaseBaseDirPath(),
        "cases/resource/codegen_t1_one_row.yaml", &table_info));
    ASSERT_TRUE(
        SqlCase::ExtractTableDef(table_info.schema_, table_info.index_, table));
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_encode", *ctx);
    CodeGenContextBase codegen_ctx(m.get());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function* fn = Function::Create(
        FunctionType::get(Type::getVoidTy(*ctx),
                          {Type::getInt8PtrTy(*ctx)->getPointerTo()}, false),
        Function::ExternalLinkage, "fn", m.get());
    {
        FunctionScopeGuard fg(fn, &codegen_ctx);
        auto& builder = *codegen_ctx.GetBuilder();
        ScopeVar sv;
        std::map<uint32_t, NativeValue> outputs;
        outputs.insert(std::make_pair(0, NativeValue::Create(builder.getInt32(32))));
        outputs.insert(std::make_pair(1, NativeValue::Create(builder.getInt16(16))));
        outputs.insert(std::make_pair(2, NativeValue::Create(::llvm::ConstantFP::get(*ctx, ::llvm::APFloat(32.1f)))));
        outputs.insert(std::make_pair(3, NativeValue::Create(::llvm::ConstantFP::get(*ctx, ::llvm::APFloat(64.1)))));
        outputs.insert(std::make_pair(4, NativeValue::Create(builder.getInt64(64))));

        std::string hello = "hello";
        ::llvm::Value* string_ref = NULL;
        bool ok = GetConstFeString(hello, codegen_ctx.GetCurrentBlock(), &string_ref);
        ASSERT_TRUE(ok);
        outputs.insert(std::make_pair(5, NativeValue::Create(string_ref)));
        outputs.insert(std::make_pair(6, NativeValue::Create(builder.getInt64(1590115420000L))));

        BufNativeEncoderIRBuilder buf_encoder_builder(&codegen_ctx, &outputs, &table.columns());
        ASSERT_TRUE(buf_encoder_builder.Init().isOK());
        Function::arg_iterator it = fn->arg_begin();
        Argument* arg0 = &*it;
        ASSERT_TRUE(buf_encoder_builder.BuildEncode(arg0).isOK());
        builder.CreateRetVoid();
        m->print(::llvm::errs(), NULL);
    }

    auto jit = std::unique_ptr<vm::HybridSeJitWrapper>(
        vm::HybridSeJitWrapper::Create());
    jit->Init();
    ASSERT_TRUE(jit->AddModule(std::move(m), std::move(ctx)));
    auto load_fn_jit = jit->FindFunction("fn");
    void (*decode)(int8_t**) =
        reinterpret_cast<void (*)(int8_t**)>(const_cast<int8_t*>(load_fn_jit));
    decode(output_ptr);
}
template <class T>
void LoadValue(T* result, bool* is_null,
               ::hybridse::type::TableDef& table,  // NOLINT
               const ::hybridse::type::Type& type, const std::string& col,
               int8_t* row, int32_t row_size) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_buf", *ctx);
    CodeGenContextBase codegen_ctx(m.get());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    ::llvm::Type* retTy = NULL;
    switch (type) {
        case ::hybridse::type::kInt16:
            retTy = Type::getInt16Ty(*ctx);
            break;
        case ::hybridse::type::kInt32:
            retTy = Type::getInt32Ty(*ctx);
            break;
        case ::hybridse::type::kInt64:
            retTy = Type::getInt64Ty(*ctx);
            break;
        case ::hybridse::type::kDouble:
            retTy = Type::getDoubleTy(*ctx);
            break;
        case ::hybridse::type::kFloat:
            retTy = Type::getFloatTy(*ctx);
            break;
        case ::hybridse::type::kVarchar: {
            const node::TypeNode type_node(hybridse::node::kVarchar);
            ASSERT_TRUE(codegen::GetLlvmType(m.get(), &type_node, &retTy));
            break;
        }
        case ::hybridse::type::kTimestamp: {
            const node::TypeNode type_node(hybridse::node::kTimestamp);
            ASSERT_TRUE(codegen::GetLlvmType(m.get(), &type_node, &retTy));
            break;
        }
        default:
            retTy = Type::getVoidTy(*ctx);
    }

    if (!retTy->isPointerTy()) {
        retTy = retTy->getPointerTo();
    }
    Function* fn = Function::Create(FunctionType::get(llvm::Type::getInt1Ty(*ctx),
                                                      {Type::getInt8PtrTy(*ctx), Type::getInt32Ty(*ctx), retTy}, false),
                                    Function::ExternalLinkage, "fn", m.get());
    {
        FunctionScopeGuard fg(fn, &codegen_ctx);
        auto& builder = *codegen_ctx.GetBuilder();
        ScopeVar sv;
        codec::MultiSlicesRowFormat buf_format(&table.columns());
        BufNativeIRBuilder buf_builder(&codegen_ctx, 0, &buf_format, &sv);
        Function::arg_iterator it = fn->arg_begin();
        Argument* arg0 = &*it;
        ++it;
        Argument* arg1 = &*it;
        ++it;
        Argument* arg2 = &*it;

        NativeValue val;
        int col_idx = -1;
        for (int i = 0; i < table.columns_size(); ++i) {
            if (table.columns(i).name() == col) {
                col_idx = i;
                break;
            }
        }
        ASSERT_GE(col_idx, 0);
        bool ok = buf_builder.BuildGetField(col_idx, arg0, arg1, &val);
        ASSERT_TRUE(ok);

        // if null
        if (val.IsNullable()) {
            llvm::BasicBlock* null_branch_block = llvm::BasicBlock::Create(*ctx);
            null_branch_block->insertInto(fn);

            llvm::BasicBlock* nonnull_branch_block = llvm::BasicBlock::Create(*ctx);
            nonnull_branch_block->insertInto(fn);

            ::llvm::Value* flag = val.GetIsNull(&builder);
            builder.CreateCondBr(flag, null_branch_block, nonnull_branch_block);

            builder.SetInsertPoint(null_branch_block);
            builder.CreateRet(llvm::ConstantInt::getTrue(*ctx));
            builder.SetInsertPoint(nonnull_branch_block);
        }

        llvm::Value* raw = val.GetValue(&builder);
        switch (type) {
            case ::hybridse::type::kVarchar: {
                codegen::StringIRBuilder string_builder(m.get());
                ASSERT_TRUE(string_builder.CopyFrom(builder.GetInsertBlock(), raw, arg2));
                break;
            }
            case ::hybridse::type::kTimestamp: {
                codegen::TimestampIRBuilder timestamp_builder(m.get());
                ::llvm::Value* ts_output;
                ASSERT_TRUE(timestamp_builder.GetTs(builder.GetInsertBlock(), raw, &ts_output));
                ASSERT_TRUE(timestamp_builder.SetTs(builder.GetInsertBlock(), arg2, ts_output));
                break;
            }
            default: {
                builder.CreateStore(raw, arg2);
            }
        }
        builder.CreateRet(llvm::ConstantInt::getFalse(*ctx));
        m->print(::llvm::errs(), NULL);
    }

    auto jit = std::unique_ptr<vm::HybridSeJitWrapper>(
        vm::HybridSeJitWrapper::Create());
    jit->Init();
    ASSERT_TRUE(jit->AddModule(std::move(m), std::move(ctx)));
    auto load_fn_jit = jit->FindFunction("fn");

    bool (*decode)(int8_t*, int32_t, T*) =
        reinterpret_cast<bool (*)(int8_t*, int32_t, T*)>(
            const_cast<int8_t*>(load_fn_jit));
    bool n = decode(row, row_size, result);
    if (is_null != nullptr) {
        *is_null = n;
    }
}

template <class T>
void RunCaseV1(T expected,
               ::hybridse::type::TableDef& table,  // NOLINT
               const ::hybridse::type::Type& type, const std::string& col,
               int8_t* row, int32_t row_size) {
    T result;
    bool is_null;
    LoadValue(&result, &is_null, table, type, col, row, row_size);
    ASSERT_EQ(is_null, false);
    ASSERT_EQ(result, expected);
}

template <class T>
void RunColCase(T expected, type::TableDef& table,  // NOLINT
                const ::hybridse::type::Type& type, const std::string& col,
                int8_t* window) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_buf", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    bool is_void = false;
    ::llvm::Type* retTy = NULL;
    switch (type) {
        case ::hybridse::type::kInt16:
            retTy = Type::getInt16Ty(*ctx);
            break;
        case ::hybridse::type::kInt32:
            retTy = Type::getInt32Ty(*ctx);
            break;
        case ::hybridse::type::kInt64:
            retTy = Type::getInt64Ty(*ctx);
            break;
        case ::hybridse::type::kDouble:
            retTy = Type::getDoubleTy(*ctx);
            break;
        case ::hybridse::type::kFloat:
            retTy = Type::getFloatTy(*ctx);
            break;
        case ::hybridse::type::kVarchar:
            retTy = Type::getInt32Ty(*ctx);
            break;
        case ::hybridse::type::kTimestamp:
            retTy = Type::getInt64PtrTy(*ctx);
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

    vm::SchemasContext schemas_context;
    schemas_context.BuildTrivial(table.catalog(),  {&table});
    size_t schema_idx;
    size_t col_idx;

    ASSERT_TRUE(
        schemas_context.ResolveColumnIndexByName("", "", col, &schema_idx, &col_idx)
            .isOK());

    MemoryWindowDecodeIRBuilder buf_builder(&schemas_context, entry_block);

    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;
    ::llvm::Value* val = NULL;
    bool ok = buf_builder.BuildGetCol(schema_idx, col_idx, arg0, &val);
    ASSERT_TRUE(ok);

    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
    ::llvm::Value* i8_ptr = builder.CreatePointerCast(val, i8_ptr_ty);
    llvm::FunctionCallee callee;
    switch (type) {
        case hybridse::type::kInt16:
            callee = m->getOrInsertFunction("print_list_i16", retTy, i8_ptr_ty);
            break;
        case hybridse::type::kInt32:
            callee = m->getOrInsertFunction("print_list_i32", retTy, i8_ptr_ty);
            break;
        case hybridse::type::kInt64:
            callee = m->getOrInsertFunction("print_list_i64", retTy, i8_ptr_ty);
            break;
        case hybridse::type::kTimestamp:
            callee = m->getOrInsertFunction("print_list_timestamp", retTy,
                                            i8_ptr_ty);
            break;
        case hybridse::type::kFloat:
            callee =
                m->getOrInsertFunction("print_list_float", retTy, i8_ptr_ty);
            break;
        case hybridse::type::kDouble:
            callee =
                m->getOrInsertFunction("print_list_double", retTy, i8_ptr_ty);
            break;
        case hybridse::type::kVarchar:
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

    auto jit = std::unique_ptr<vm::HybridSeJitWrapper>(
        vm::HybridSeJitWrapper::Create());
    jit->Init();
    ASSERT_TRUE(jit->AddModule(std::move(m), std::move(ctx)));
    jit->AddExternalFunction("print_list_i16",
                             reinterpret_cast<void*>(&PrintListInt16));
    jit->AddExternalFunction("print_list_i32",
                             reinterpret_cast<void*>(&PrintListInt32));
    jit->AddExternalFunction("print_list_i64",
                             reinterpret_cast<void*>(&PrintListInt64));
    jit->AddExternalFunction("print_list_float",
                             reinterpret_cast<void*>(&PrintListFloat));
    jit->AddExternalFunction("print_list_double",
                             reinterpret_cast<void*>(&PrintListDouble));
    jit->AddExternalFunction("print_list_string",
                             reinterpret_cast<void*>(&PrintListString));
    jit->AddExternalFunction("print_list_timestamp",
                             reinterpret_cast<void*>(&PrintListTimestamp));

    auto load_fn_jit = jit->FindFunction("fn");
    codec::ListRef<> window_ref;
    window_ref.list = window;
    if (!is_void) {
        T(*decode)
        (int8_t*) =
            reinterpret_cast<T (*)(int8_t*)>(const_cast<int8_t*>(load_fn_jit));
        ASSERT_EQ(expected, decode(reinterpret_cast<int8_t*>(&window_ref)));

    } else {
        void (*decode)(int8_t*) = reinterpret_cast<void (*)(int8_t*)>(
            const_cast<int8_t*>(load_fn_jit));
        decode(reinterpret_cast<int8_t*>(&window_ref));
    }
}

static bool operator==(const openmldb::base::Timestamp& a, const openmldb::base::Timestamp& b) {
    return a.ts_ == b.ts_;
}
static bool operator!=(const openmldb::base::Timestamp& a, const openmldb::base::Timestamp& b) {
    return a.ts_ != b.ts_;
}

TEST_F(BufIRBuilderTest, native_test_load_int16) {
    int8_t* ptr = NULL;
    uint32_t size = 0;
    type::TableDef table;
    BuildT1Buf(table, &ptr, &size);
    RunCaseV1<int16_t>(16, table, ::hybridse::type::kInt16, "col2", ptr, size);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_string) {
    int8_t* ptr = NULL;
    uint32_t size = 0;
    type::TableDef table;
    BuildT1Buf(table, &ptr, &size);
    RunCaseV1<openmldb::base::StringRef>(openmldb::base::StringRef(strlen("1"), strdup("1")),
                                table, ::hybridse::type::kVarchar, "col6", ptr,
                                size);
    free(ptr);
}

TEST_F(BufIRBuilderTest, encode_ir_builder) {
    int8_t* ptr = NULL;
    type::TableDef table;
    RunEncode(table, &ptr);
    bool ok = ptr != NULL;
    ASSERT_TRUE(ok);

    codec::RowView row_view(table.columns());
    row_view.Reset(ptr);

    ASSERT_EQ(32, row_view.GetInt32Unsafe(0));
    ASSERT_EQ(16, row_view.GetInt16Unsafe(1));
    ASSERT_EQ(32.1f, row_view.GetFloatUnsafe(2));
    ASSERT_EQ(64.1, row_view.GetDoubleUnsafe(3));
    ASSERT_EQ(64, row_view.GetInt64Unsafe(4));
    ASSERT_EQ("hello", row_view.GetStringUnsafe(5));
    ASSERT_EQ(1590115420000L, row_view.GetTimestampUnsafe(6));
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_int16_col) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindowFromResource("cases/resource/codegen_t1_five_row.yaml", table,
                            rows, &ptr);
    RunColCase<int16_t>(16 * 5, table, ::hybridse::type::kInt16, "col2", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_int32_col) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindowFromResource("cases/resource/codegen_t1_five_row.yaml", table,
                            rows, &ptr);
    RunColCase<int32_t>(32 * 5, table, ::hybridse::type::kInt32, "col1", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_int64_col) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindowFromResource("cases/resource/codegen_t1_five_row.yaml", table,
                            rows, &ptr);
    RunColCase<int64_t>(64 * 5, table, ::hybridse::type::kInt64, "col5", ptr);
    free(ptr);
}
TEST_F(BufIRBuilderTest, native_test_load_timestamp_col) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindowFromResource("cases/resource/codegen_t1_five_row.yaml", table,
                            rows, &ptr);
    RunColCase<int64_t>(1590115420000L * 5, table, ::hybridse::type::kTimestamp,
                        "std_ts", ptr);
    free(ptr);
}
TEST_F(BufIRBuilderTest, native_test_load_float_col) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindowFromResource("cases/resource/codegen_t1_five_row.yaml", table,
                            rows, &ptr);
    RunColCase<float>(2.1f * 5, table, ::hybridse::type::kFloat, "col3", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_double_col) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindowFromResource("cases/resource/codegen_t1_five_row.yaml", table,
                            rows, &ptr);
    RunColCase<double>(3.1f * 5, table, ::hybridse::type::kDouble, "col4", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, native_test_load_string_col) {
    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindowFromResource("cases/resource/codegen_t1_five_row.yaml", table,
                            rows, &ptr);
    RunColCase<int32_t>(5, table, ::hybridse::type::kVarchar, "col6", ptr);
    free(ptr);
}

TEST_F(BufIRBuilderTest, spark_unsaferow_native_test_load_string) {
    FLAGS_enable_spark_unsaferow_format = true;

    int8_t* ptr = NULL;
    uint32_t size = 0;
    type::TableDef table;
    BuildT1Buf(table, &ptr, &size);

    openmldb::base::StringRef result;
    bool is_null;
    LoadValue(&result, &is_null, table, ::hybridse::type::kVarchar, "col6", ptr, size);
    ASSERT_EQ(is_null, false);

    free(ptr);

    FLAGS_enable_spark_unsaferow_format = false;
}
TEST_F(BufIRBuilderTest, spark_unsaferow_native_test_load_int64_col) {
    FLAGS_enable_spark_unsaferow_format = true;

    int8_t* ptr = NULL;
    std::vector<Row> rows;
    type::TableDef table;
    BuildWindowFromResource("cases/resource/codegen_t1_five_row.yaml", table,
                            rows, &ptr);
    RunColCase<int64_t>(64 * 5, table, ::hybridse::type::kInt64, "col5", ptr);
    free(ptr);

    FLAGS_enable_spark_unsaferow_format = false;
}
TEST_F(BufIRBuilderTest, spark_unsaferow_encode_ir_builder) {
    FLAGS_enable_spark_unsaferow_format = true;

    int8_t* ptr = NULL;
    type::TableDef table;
    RunEncode(table, &ptr);
    bool ok = ptr != NULL;
    ASSERT_TRUE(ok);

    codec::RowView row_view(table.columns());
    row_view.Reset(ptr);

    ASSERT_EQ(32, row_view.GetInt32Unsafe(0));
    ASSERT_EQ(16, row_view.GetInt16Unsafe(1));
    ASSERT_EQ(32.1f, row_view.GetFloatUnsafe(2));
    ASSERT_EQ(64.1, row_view.GetDoubleUnsafe(3));
    ASSERT_EQ(64, row_view.GetInt64Unsafe(4));
    free(ptr);

    FLAGS_enable_spark_unsaferow_format = false;
}

}  // namespace codegen
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
