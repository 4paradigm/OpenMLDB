/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * cast_expr_ir_builder_test.cc
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 **/
#include "codegen/cast_expr_ir_builder.h"
#include <memory>
#include <string>
#include <utility>
#include "codegen/ir_base_builder.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT
ExitOnError ExitOnErr;
namespace fesql {
namespace codegen {
class CastExprIrBuilderTest : public ::testing::Test {
 public:
    CastExprIrBuilderTest() { manager_ = new node::NodeManager(); }
    ~CastExprIrBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

void CastErrorCheck(::fesql::node::DataType src_type,
                    ::fesql::node::DataType dist_type, bool safe,
                    const std::string &msg) {
    // Create an LLJIT instance.
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("cast_int_2_long", *ctx);
    llvm::Type *src_llvm_type = NULL;
    llvm::Type *dist_llvm_type = NULL;
    ASSERT_TRUE(
        ::fesql::codegen::GetLLVMType(m.get(), src_type, &src_llvm_type));
    ASSERT_TRUE(GetLLVMType(m.get(), dist_type, &dist_llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn = Function::Create(
        FunctionType::get(dist_llvm_type, {src_llvm_type}, false),
        Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin();
    ScopeVar scope_var;
    scope_var.Enter("fn_base");
    scope_var.AddVar("a", arg0);
    CastExprIRBuilder cast_expr_ir_builder(entry_block);
    llvm::Value *output;
    base::Status status;
    bool ok = safe ? cast_expr_ir_builder.SafeCast(arg0, dist_llvm_type,
                                                   &output, status)
                   : cast_expr_ir_builder.UnSafeCast(arg0, dist_llvm_type,
                                                     &output, status);
    ASSERT_FALSE(ok);
    ASSERT_EQ(status.msg, msg);
}

template <class S, class D>
void CastCheck(::fesql::node::DataType src_type,
               ::fesql::node::DataType dist_type, S value, D cast_value,
               bool safe) {
    // Create an LLJIT instance.
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("cast_int_2_long", *ctx);
    llvm::Type *src_llvm_type = NULL;
    llvm::Type *dist_llvm_type = NULL;
    ASSERT_TRUE(
        ::fesql::codegen::GetLLVMType(m.get(), src_type, &src_llvm_type));
    ASSERT_TRUE(GetLLVMType(m.get(), dist_type, &dist_llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn = Function::Create(
        FunctionType::get(dist_llvm_type, {src_llvm_type}, false),
        Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin();
    ScopeVar scope_var;
    scope_var.Enter("fn_base");
    scope_var.AddVar("a", arg0);
    CastExprIRBuilder cast_expr_ir_builder(entry_block);
    llvm::Value *output;
    base::Status status;
    bool ok = safe ? cast_expr_ir_builder.SafeCast(arg0, dist_llvm_type,
                                                   &output, status)
                   : cast_expr_ir_builder.UnSafeCast(arg0, dist_llvm_type,
                                                     &output, status);
    ASSERT_TRUE(ok);
    switch (dist_type) {
        case ::fesql::node::kInt16:
        case ::fesql::node::kInt32:
        case ::fesql::node::kInt64: {
            ::llvm::Value *output_mul_4 = builder.CreateAdd(
                builder.CreateAdd(builder.CreateAdd(output, output), output),
                output);
            builder.CreateRet(output_mul_4);
            break;
        }
        case ::fesql::node::kFloat:
        case ::fesql::node::kDouble: {
            ::llvm::Value *output_mul_4 = builder.CreateFAdd(
                builder.CreateFAdd(builder.CreateFAdd(output, output), output),
                output);
            builder.CreateRet(output_mul_4);
            break;
        }
        default: {
            FAIL();
        }
    }
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    D (*decode)(S) = (D(*)(S))load_fn_jit.getAddress();
    D ret = decode(value);
    ASSERT_EQ(ret, cast_value);
}

template <class S, class D>
void UnSafeCastCheck(::fesql::node::DataType src_type,
                     ::fesql::node::DataType dist_type, S value, D cast_value) {
    CastCheck<S, D>(src_type, dist_type, value, cast_value, false);
}

template <class S, class D>
void SafeCastCheck(::fesql::node::DataType src_type,
                   ::fesql::node::DataType dist_type, S value, D cast_value) {
    CastCheck<S, D>(src_type, dist_type, value, cast_value, true);
}

void SafeCastErrorCheck(::fesql::node::DataType src_type,
                        ::fesql::node::DataType dist_type, std::string msg) {
    CastErrorCheck(src_type, dist_type, true, msg);
}

template <class V>
void BoolCastCheck(::fesql::node::DataType type, V value, bool result) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("bool_cast_func", *ctx);

    llvm::Type *left_llvm_type = NULL;
    llvm::Type *dist_llvm_type = NULL;
    ASSERT_TRUE(::fesql::codegen::GetLLVMType(m.get(), type, &left_llvm_type));
    ASSERT_TRUE(GetLLVMType(m.get(), ::fesql::node::kBool, &dist_llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn = Function::Create(
        FunctionType::get(dist_llvm_type, {left_llvm_type}, false),
        Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    ScopeVar scope_var;
    scope_var.Enter("fn_base");
    scope_var.AddVar("a", arg0);
    CastExprIRBuilder ir_builder(entry_block);
    llvm::Value *output;
    base::Status status;

    bool ok;

    ok = ir_builder.BoolCast(arg0, &output, status);
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    ASSERT_TRUE(ok);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    bool (*decode)(V) = (bool (*)(V))load_fn_jit.getAddress();
    bool ret = decode(value);
    ASSERT_EQ(ret, result);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test) {
    UnSafeCastCheck<int16_t, int16_t>(::fesql::node::kInt16,
                                      ::fesql::node::kInt16, 1u, 4u);
    UnSafeCastCheck<int16_t, int32_t>(::fesql::node::kInt16,
                                      ::fesql::node::kInt32, 10000u, 40000);
    UnSafeCastCheck<int16_t, int64_t>(::fesql::node::kInt16,
                                      ::fesql::node::kInt64, 10000u, 40000L);
    UnSafeCastCheck<int16_t, float>(::fesql::node::kInt16,
                                    ::fesql::node::kFloat, 10000u, 40000.0f);
    UnSafeCastCheck<int16_t, double>(::fesql::node::kInt16,
                                     ::fesql::node::kDouble, 10000u, 40000.0);

    UnSafeCastCheck<int32_t, int16_t>(::fesql::node::kInt32,
                                      ::fesql::node::kInt16, 1, 4u);
    UnSafeCastCheck<int32_t, int32_t>(::fesql::node::kInt32,
                                      ::fesql::node::kInt32, 1, 4);
    UnSafeCastCheck<int32_t, int64_t>(
        ::fesql::node::kInt32, ::fesql::node::kInt64, 2000000000, 8000000000L);
    UnSafeCastCheck<int32_t, float>(::fesql::node::kInt32,
                                    ::fesql::node::kFloat, 1, 4.0f);
    UnSafeCastCheck<int32_t, double>(::fesql::node::kInt32,
                                     ::fesql::node::kDouble, 2000000000,
                                     8000000000.0);

    UnSafeCastCheck<float, int16_t>(::fesql::node::kFloat,
                                    ::fesql::node::kInt16, 1.5f, 4u);
    UnSafeCastCheck<float, int32_t>(::fesql::node::kFloat,
                                    ::fesql::node::kInt32, 10000.5f, 40000);
    UnSafeCastCheck<float, int64_t>(::fesql::node::kFloat,
                                    ::fesql::node::kInt64, 2000000000.5f,
                                    8000000000L);
    UnSafeCastCheck<float, double>(::fesql::node::kFloat,
                                   ::fesql::node::kDouble, 2000000000.5f,
                                   static_cast<double>(2000000000.5f) * 4.0);
}

TEST_F(CastExprIrBuilderTest, safe_cast_error_test) {
    SafeCastErrorCheck(::fesql::node::kInt32, ::fesql::node::kInt16,
                       "unsafe cast");

    SafeCastErrorCheck(::fesql::node::kInt64, ::fesql::node::kInt16,
                       "unsafe cast");
    SafeCastErrorCheck(::fesql::node::kInt64, ::fesql::node::kInt32,
                       "unsafe cast");
    SafeCastErrorCheck(::fesql::node::kInt64, ::fesql::node::kFloat,
                       "unsafe cast");
    SafeCastErrorCheck(::fesql::node::kInt64, ::fesql::node::kDouble,
                       "unsafe cast");

    SafeCastErrorCheck(::fesql::node::kFloat, ::fesql::node::kInt16,
                       "unsafe cast");
    SafeCastErrorCheck(::fesql::node::kFloat, ::fesql::node::kInt32,
                       "unsafe cast");
    SafeCastErrorCheck(::fesql::node::kFloat, ::fesql::node::kInt64,
                       "unsafe cast");

    SafeCastErrorCheck(::fesql::node::kDouble, ::fesql::node::kInt16,
                       "unsafe cast");
    SafeCastErrorCheck(::fesql::node::kDouble, ::fesql::node::kInt32,
                       "unsafe cast");
    SafeCastErrorCheck(::fesql::node::kDouble, ::fesql::node::kInt64,
                       "unsafe cast");
    SafeCastErrorCheck(::fesql::node::kDouble, ::fesql::node::kFloat,
                       "unsafe cast");
}

TEST_F(CastExprIrBuilderTest, bool_cast_test) {
    BoolCastCheck<int16_t>(::fesql::node::kInt16, 1u, true);
    BoolCastCheck<int32_t>(::fesql::node::kInt32, 1, true);
    BoolCastCheck<int64_t>(::fesql::node::kInt64, 1, true);
    BoolCastCheck<float>(::fesql::node::kFloat, 1.0f, true);
    BoolCastCheck<double>(::fesql::node::kDouble, 1.0, true);

    BoolCastCheck<int16_t>(::fesql::node::kInt16, 0, false);
    BoolCastCheck<int32_t>(::fesql::node::kInt32, 0, false);
    BoolCastCheck<int64_t>(::fesql::node::kInt64, 0, false);
    BoolCastCheck<float>(::fesql::node::kFloat, 0.0f, false);
    BoolCastCheck<double>(::fesql::node::kDouble, 0.0, false);
}

}  // namespace codegen
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
