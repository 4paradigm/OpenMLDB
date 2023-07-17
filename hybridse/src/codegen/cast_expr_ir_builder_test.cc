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

#include "codegen/cast_expr_ir_builder.h"
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "case/sql_case.h"
#include "codegen/ir_base_builder.h"
#include "codegen/ir_base_builder_test.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT
using hybridse::udf::Nullable;
ExitOnError ExitOnErr;
namespace hybridse {
namespace codegen {
using openmldb::base::StringRef;
using openmldb::base::Date;
using openmldb::base::Timestamp;
class CastExprIrBuilderTest : public ::testing::Test {
 public:
    CastExprIrBuilderTest() { manager_ = new node::NodeManager(); }
    ~CastExprIrBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

void CastErrorCheck(::hybridse::node::DataType src_type,
                    ::hybridse::node::DataType dist_type, bool safe,
                    const std::string &msg) {
    // Create an LLJIT instance.
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("cast_int_2_long", *ctx);
    llvm::Type *src_llvm_type = NULL;
    llvm::Type *dist_llvm_type = NULL;
    ASSERT_TRUE(
        ::hybridse::codegen::GetLlvmType(m.get(), src_type, &src_llvm_type));
    ASSERT_TRUE(GetLlvmType(m.get(), dist_type, &dist_llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn = Function::Create(
        FunctionType::get(dist_llvm_type, {src_llvm_type}, false),
        Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin();
    ScopeVar scope_var;
    scope_var.AddVar("a", NativeValue::Create(arg0));
    CastExprIRBuilder cast_expr_ir_builder(entry_block);
    llvm::Value *output;
    base::Status status;

    if (safe) {
        ASSERT_FALSE(
            cast_expr_ir_builder.IsSafeCast(src_llvm_type, dist_llvm_type));
        return;
    }

    bool ok = cast_expr_ir_builder.UnSafeCastNumber(arg0, dist_llvm_type,
                                                    &output, status);
    ASSERT_FALSE(ok) << "Fail cast from " << node::DataTypeName(src_type)
                     << " to " << node::DataTypeName(dist_type);
    //    ASSERT_EQ(status.msg, msg);
}

template <class S, class D>
void CastCheck(::hybridse::node::DataType src_type,
               ::hybridse::node::DataType dist_type, S value, D cast_value,
               bool safe) {
    // Create an LLJIT instance.
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("cast_int_2_long", *ctx);
    llvm::Type *src_llvm_type = NULL;
    llvm::Type *dist_llvm_type = NULL;
    ASSERT_TRUE(
        ::hybridse::codegen::GetLlvmType(m.get(), src_type, &src_llvm_type));
    ASSERT_TRUE(GetLlvmType(m.get(), dist_type, &dist_llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn = Function::Create(
        FunctionType::get(dist_llvm_type, {src_llvm_type}, false),
        Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin();
    ScopeVar scope_var;
    scope_var.AddVar("a", NativeValue::Create(arg0));
    CastExprIRBuilder cast_expr_ir_builder(entry_block);
    llvm::Value *output;
    base::Status status;
    bool ok = safe ? cast_expr_ir_builder.SafeCastNumber(arg0, dist_llvm_type,
                                                         &output, status)
                   : cast_expr_ir_builder.UnSafeCastNumber(arg0, dist_llvm_type,
                                                           &output, status);
    ASSERT_TRUE(ok);
    switch (dist_type) {
        case ::hybridse::node::kInt16:
        case ::hybridse::node::kInt32:
        case ::hybridse::node::kInt64: {
            ::llvm::Value *output_mul_4 = builder.CreateAdd(
                builder.CreateAdd(builder.CreateAdd(output, output), output),
                output);
            builder.CreateRet(output_mul_4);
            break;
        }
        case ::hybridse::node::kFloat:
        case ::hybridse::node::kDouble: {
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
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    D (*decode)(S) = (D(*)(S))load_fn_jit.getAddress();
    D ret = decode(value);
    ASSERT_EQ(ret, cast_value);
}

template <class S, class D>
void UnSafeCastCheck(::hybridse::node::DataType src_type,
                     ::hybridse::node::DataType dist_type, S value,
                     D cast_value) {
    CastCheck<S, D>(src_type, dist_type, value, cast_value, false);
}

template <class S, class D>
void SafeCastCheck(::hybridse::node::DataType src_type,
                   ::hybridse::node::DataType dist_type, S value,
                   D cast_value) {
    CastCheck<S, D>(src_type, dist_type, value, cast_value, true);
}

void SafeCastErrorCheck(::hybridse::node::DataType src_type,
                        ::hybridse::node::DataType dist_type, std::string msg) {
    CastErrorCheck(src_type, dist_type, true, msg);
}

template <class V>
void BoolCastCheck(::hybridse::node::DataType type, V value, bool result) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("bool_cast_func", *ctx);

    llvm::Type *left_llvm_type = NULL;
    llvm::Type *dist_llvm_type = NULL;
    ASSERT_TRUE(
        ::hybridse::codegen::GetLlvmType(m.get(), type, &left_llvm_type));
    ASSERT_TRUE(GetLlvmType(m.get(), ::hybridse::node::kBool, &dist_llvm_type));

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
    scope_var.AddVar("a", NativeValue::Create(arg0));
    CastExprIRBuilder ir_builder(entry_block);
    llvm::Value *output;
    base::Status status;

    bool ok;

    ok = ir_builder.BoolCast(arg0, &output, status);
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    ASSERT_TRUE(ok);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    bool (*decode)(V) = (bool (*)(V))load_fn_jit.getAddress();
    bool ret = decode(value);
    ASSERT_EQ(ret, result);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_1) {
    UnSafeCastCheck<int16_t, int16_t>(::hybridse::node::kInt16,
                                      ::hybridse::node::kInt16, 1u, 4u);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_2) {
    UnSafeCastCheck<int16_t, int32_t>(::hybridse::node::kInt16,
                                      ::hybridse::node::kInt32, 10000u, 40000);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_3) {
    UnSafeCastCheck<int16_t, int64_t>(::hybridse::node::kInt16,
                                      ::hybridse::node::kInt64, 10000u, 40000L);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_4) {
    UnSafeCastCheck<int16_t, float>(::hybridse::node::kInt16,
                                    ::hybridse::node::kFloat, 10000u, 40000.0f);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_5) {
    UnSafeCastCheck<int16_t, double>(
        ::hybridse::node::kInt16, ::hybridse::node::kDouble, 10000u, 40000.0);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_6) {
    UnSafeCastCheck<int32_t, int16_t>(::hybridse::node::kInt32,
                                      ::hybridse::node::kInt16, 1, 4u);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_7) {
    UnSafeCastCheck<int32_t, int32_t>(::hybridse::node::kInt32,
                                      ::hybridse::node::kInt32, 1, 4);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_8) {
    UnSafeCastCheck<int32_t, int64_t>(::hybridse::node::kInt32,
                                      ::hybridse::node::kInt64, 2000000000,
                                      8000000000L);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_9) {
    UnSafeCastCheck<int32_t, float>(::hybridse::node::kInt32,
                                    ::hybridse::node::kFloat, 1, 4.0f);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_10) {
    UnSafeCastCheck<int32_t, double>(::hybridse::node::kInt32,
                                     ::hybridse::node::kDouble, 2000000000,
                                     8000000000.0);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_11) {
    UnSafeCastCheck<float, int16_t>(::hybridse::node::kFloat,
                                    ::hybridse::node::kInt16, 1.5f, 4u);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_12) {
    UnSafeCastCheck<float, int32_t>(::hybridse::node::kFloat,
                                    ::hybridse::node::kInt32, 10000.5f, 40000);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_13) {
    UnSafeCastCheck<float, int64_t>(::hybridse::node::kFloat,
                                    ::hybridse::node::kInt64, 2000000000.5f,
                                    8000000000L);
}
TEST_F(CastExprIrBuilderTest, unsafe_cast_test_14) {
    UnSafeCastCheck<float, double>(::hybridse::node::kFloat,
                                   ::hybridse::node::kDouble, 2000000000.5f,
                                   static_cast<double>(2000000000.5f) * 4.0);
}

TEST_F(CastExprIrBuilderTest, safe_cast_error_test_0) {
    SafeCastErrorCheck(::hybridse::node::kInt32, ::hybridse::node::kInt16,
                       "unsafe");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_1) {
    SafeCastErrorCheck(::hybridse::node::kInt64, ::hybridse::node::kInt16,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_2) {
    SafeCastErrorCheck(::hybridse::node::kInt64, ::hybridse::node::kInt32,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_3) {
    SafeCastErrorCheck(::hybridse::node::kInt64, ::hybridse::node::kFloat,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_4) {
    SafeCastErrorCheck(::hybridse::node::kInt64, ::hybridse::node::kDouble,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_5) {
    SafeCastErrorCheck(::hybridse::node::kFloat, ::hybridse::node::kInt16,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_6) {
    SafeCastErrorCheck(::hybridse::node::kFloat, ::hybridse::node::kInt32,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_7) {
    SafeCastErrorCheck(::hybridse::node::kFloat, ::hybridse::node::kInt64,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_8) {
    SafeCastErrorCheck(::hybridse::node::kDouble, ::hybridse::node::kInt16,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_9) {
    SafeCastErrorCheck(::hybridse::node::kDouble, ::hybridse::node::kInt32,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_10) {
    SafeCastErrorCheck(::hybridse::node::kDouble, ::hybridse::node::kInt64,
                       "unsafe cast");
}
TEST_F(CastExprIrBuilderTest, safe_cast_error_test_11) {
    SafeCastErrorCheck(::hybridse::node::kDouble, ::hybridse::node::kFloat,
                       "unsafe cast");
}

TEST_F(CastExprIrBuilderTest, bool_cast_test) {
    BoolCastCheck<int16_t>(::hybridse::node::kInt16, 1u, true);
    BoolCastCheck<int32_t>(::hybridse::node::kInt32, 1, true);
    BoolCastCheck<int64_t>(::hybridse::node::kInt64, 1, true);
    BoolCastCheck<float>(::hybridse::node::kFloat, 1.0f, true);
    BoolCastCheck<double>(::hybridse::node::kDouble, 1.0, true);

    BoolCastCheck<int16_t>(::hybridse::node::kInt16, 0, false);
    BoolCastCheck<int32_t>(::hybridse::node::kInt32, 0, false);
    BoolCastCheck<int64_t>(::hybridse::node::kInt64, 0, false);
    BoolCastCheck<float>(::hybridse::node::kFloat, 0.0f, false);
    BoolCastCheck<double>(::hybridse::node::kDouble, 0.0, false);
}

template <typename Ret, typename... Args>
void ExprCheck(
    const std::function<node::ExprNode *(
        node::NodeManager *,
        typename std::pair<Args, node::ExprNode *>::second_type...)> &expr_func,
    Ret expect, Args... args) {
    auto compiled_func = BuildExprFunction<Ret, Args...>(expr_func);
    ASSERT_TRUE(compiled_func.valid())
        << "Fail Expr Check: "
        << "Ret: " << DataTypeTrait<Ret>::to_string << "Args: ...";

    std::ostringstream oss;
    Ret result = compiled_func(args...);
    ASSERT_EQ(expect, result);
}
template <typename Ret, typename... Args>
void ExprErrorCheck(
    const std::function<node::ExprNode *(
        node::NodeManager *,
        typename std::pair<Args, node::ExprNode *>::second_type...)>
        &expr_func) {
    auto compiled_func = BuildExprFunction<Ret, Args...>(expr_func);
    ASSERT_FALSE(compiled_func.valid());
}

class CastExprTest
    : public ::testing::TestWithParam<
          std::tuple<std::string, std::string, std::string, std::string>> {
 public:
    CastExprTest() { manager_ = new node::NodeManager(); }
    ~CastExprTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <typename CASTTYPE>
void CastExprCheck(CASTTYPE exp_value, std::string src_type_str,
                   std::string src_value_str) {
    auto cast_func = [](node::NodeManager *nm, node::ExprNode *input) {
        return nm->MakeCastNode(
            DataTypeTrait<CASTTYPE>::to_type_node(nm)->base_, input);
    };

    hybridse::type::Type src_type;
    ASSERT_TRUE(hybridse::sqlcase::SqlCase::TypeParse(src_type_str, &src_type));
    switch (src_type) {
        case type::kBool: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<bool>>(cast_func, exp_value,
                                                         nullptr);
            } else {
                bool flag;
                std::istringstream ss(src_value_str);
                ss >> std::boolalpha >> flag;
                ExprCheck<CASTTYPE, udf::Nullable<bool>>(cast_func, exp_value,
                                                         flag);
            }
            break;
        }
        case type::kInt16: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<int16_t>>(cast_func,
                                                            exp_value, nullptr);
            } else {
                ExprCheck<CASTTYPE, udf::Nullable<int16_t>>(
                    cast_func, exp_value,
                    boost::lexical_cast<int16_t>(src_value_str));
            }
            break;
        }
        case type::kInt32: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<int32_t>>(cast_func,
                                                            exp_value, nullptr);
                return;
            }
            ExprCheck<CASTTYPE, udf::Nullable<int32_t>>(
                cast_func, exp_value,
                boost::lexical_cast<int32_t>(src_value_str));
            break;
        }
        case type::kInt64: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<int64_t>>(cast_func,
                                                            exp_value, nullptr);
                return;
            }
            ExprCheck<CASTTYPE, udf::Nullable<int64_t>>(
                cast_func, exp_value,
                boost::lexical_cast<int64_t>(src_value_str));
            break;
        }
        case type::kFloat: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<float>>(cast_func, exp_value,
                                                          nullptr);
                return;
            }
            ExprCheck<CASTTYPE, udf::Nullable<float>>(
                cast_func, exp_value,
                boost::lexical_cast<float>(src_value_str));
            break;
        }
        case type::kDouble: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<double>>(cast_func, exp_value,
                                                           nullptr);
                return;
            }
            ExprCheck<CASTTYPE, udf::Nullable<double>>(
                cast_func, exp_value,
                boost::lexical_cast<double>(src_value_str));
            break;
        }
        case type::kTimestamp: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<Timestamp>>(
                    cast_func, exp_value, nullptr);
                return;
            }
            ExprCheck<CASTTYPE, udf::Nullable<Timestamp>>(
                cast_func, exp_value,
                Timestamp(boost::lexical_cast<int64_t>(src_value_str)));
            break;
        }
        case type::kDate: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<Date>>(
                    cast_func, exp_value, nullptr);
                return;
            }
            std::vector<std::string> date_strs;
            boost::split(date_strs, src_value_str, boost::is_any_of("-"),
                         boost::token_compress_on);

            ExprCheck<CASTTYPE, udf::Nullable<Date>>(
                cast_func, exp_value,
                Date(boost::lexical_cast<int32_t>(date_strs[0]),
                            boost::lexical_cast<int32_t>(date_strs[1]),
                            boost::lexical_cast<int32_t>(date_strs[2])));
            break;
        }
        case type::kVarchar: {
            if ("null" == src_value_str) {
                ExprCheck<CASTTYPE, udf::Nullable<StringRef>>(
                    cast_func, exp_value, nullptr);
                return;
            }
            ExprCheck<CASTTYPE, udf::Nullable<StringRef>>(
                cast_func, exp_value, StringRef(src_value_str.size(), src_value_str.data()));
            break;
        }
        default: {
            FAIL() << "invalid casted from type "
                   << DataTypeTrait<CASTTYPE>::to_string() << " to type "
                   << type::Type_Name(src_type);
        }
    }
}

void CastExprCheck(std::string cast_type_str, std::string cast_value_str,
                   std::string src_type_str, std::string src_value_str) {
    hybridse::type::Type cast_type;
    ASSERT_TRUE(
        hybridse::sqlcase::SqlCase::TypeParse(cast_type_str, &cast_type));
    switch (cast_type) {
        case type::kBool: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<bool>>(nullptr, src_type_str,
                                                   src_value_str);
            } else {
                bool exp_value;
                std::istringstream ss(cast_value_str);
                ss >> std::boolalpha >> exp_value;
                CastExprCheck<bool>(exp_value, src_type_str, src_value_str);
            }
            break;
        }
        case type::kInt16: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<int16_t>>(nullptr, src_type_str,
                                                      src_value_str);
            } else {
                int16_t exp_value =
                    boost::lexical_cast<int16_t>(cast_value_str);
                CastExprCheck<int16_t>(exp_value, src_type_str, src_value_str);
            }
            break;
        }
        case type::kInt32: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<int32_t>>(nullptr, src_type_str,
                                                      src_value_str);
            } else {
                int32_t exp_value =
                    boost::lexical_cast<int32_t>(cast_value_str);
                CastExprCheck<int32_t>(exp_value, src_type_str, src_value_str);
            }
            break;
        }
        case type::kInt64: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<int64_t>>(nullptr, src_type_str,
                                                      src_value_str);
            } else {
                int64_t exp_value =
                    boost::lexical_cast<int64_t>(cast_value_str);
                CastExprCheck<int64_t>(exp_value, src_type_str, src_value_str);
            }
            break;
        }
        case type::kFloat: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<float>>(nullptr, src_type_str,
                                                    src_value_str);
            } else {
                float exp_value = boost::lexical_cast<float>(cast_value_str);
                CastExprCheck<float>(exp_value, src_type_str, src_value_str);
            }

            break;
        }
        case type::kDouble: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<double>>(nullptr, src_type_str,
                                                     src_value_str);
            } else {
                double exp_value = boost::lexical_cast<double>(cast_value_str);
                CastExprCheck<double>(exp_value, src_type_str, src_value_str);
            }
            break;
        }
        case type::kTimestamp: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<Timestamp>>(
                    nullptr, src_type_str, src_value_str);
            } else {
                int64_t exp_value =
                    boost::lexical_cast<int64_t>(cast_value_str);
                CastExprCheck<Timestamp>(Timestamp(exp_value),
                                                src_type_str, src_value_str);
            }
            break;
        }
        case type::kDate: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<Date>>(nullptr, src_type_str,
                                                          src_value_str);
            } else {
                std::vector<std::string> date_strs;
                boost::split(date_strs, cast_value_str, boost::is_any_of("-"),
                             boost::token_compress_on);
                Date exp_value =
                    Date(boost::lexical_cast<int32_t>(date_strs[0]),
                                boost::lexical_cast<int32_t>(date_strs[1]),
                                boost::lexical_cast<int32_t>(date_strs[2]));
                CastExprCheck<Date>(exp_value, src_type_str,
                                           src_value_str);
            }
            break;
        }
        case type::kVarchar: {
            if ("null" == cast_value_str) {
                CastExprCheck<udf::Nullable<StringRef>>(
                    nullptr, src_type_str, src_value_str);
            } else {
                CastExprCheck<StringRef>(
                    StringRef(cast_value_str.size(), cast_value_str.data()), src_type_str,
                    src_value_str);
            }
            break;
        }
        default: {
            FAIL() << "invalid casted from type " << type::Type_Name(cast_type);
        }
    }
}

/**
| src\|dist | bool   | smallint | int    | float  | int64  | double | timestamp
| date   | string | | :-------- | :----- | :------- | :----- | :----- | :----- |
:----- | :-------- | :----- | :----- | | bool      | Safe   | Safe     | Safe |
Safe   | Safe   | Safe   |           |        | Safe   | | smallint  | UnSafe |
Safe     | Safe   | Safe   | Safe   | Safe   |           |        | Safe   | |
int       | UnSafe | UnSafe   | Safe   | Safe   | Safe   | Safe   |           |
| Safe   | | float     | UnSafe | UnSafe   | UnSafe | Safe   | Safe   | Safe   |
|        | Safe   | | bigint    | UnSafe | UnSafe   | UnSafe | UnSafe | Safe   |
UnSafe | Y         |        | Safe   | | double    | UnSafe | UnSafe   | UnSafe
| UnSafe | UnSafe | Safe   |           |        | Safe   | | timestamp | |
UnSafe   | UnSafe | UnSafe | Safe   | UnSafe | Safe      | UnSafe | Safe   | |
date      |        |          |        |        |        |        | UnSafe    |
Safe   | Safe   | | string    |        |          |        |        |        |
| UnSafe    | UnSafe | Safe   |
 */

// SafeCastNumber: bool
// UnSafeCst: int16, int32, int64, float, double
INSTANTIATE_TEST_SUITE_P(
    CastExprTestBool, CastExprTest,
    testing::Values(std::make_tuple("bool", "true", "bool", "true"),
                    std::make_tuple("bool", "true", "int16", "1"),
                    std::make_tuple("bool", "true", "int32", "1"),
                    std::make_tuple("bool", "true", "int64", "1"),
                    std::make_tuple("bool", "true", "float", "1"),
                    std::make_tuple("bool", "true", "double", "1"),
                    std::make_tuple("bool", "true", "timestamp", "1"),
                    std::make_tuple("bool", "null", "date", "2020-05-20"),
                    std::make_tuple("bool", "true", "string", "true"),
                    std::make_tuple("bool", "true", "string", "t"),
                    std::make_tuple("bool", "true", "string", "yes"),
                    std::make_tuple("bool", "true", "string", "y"),
                    std::make_tuple("bool", "true", "string", "1"),
                    std::make_tuple("bool", "false", "bool", "false"),
                    std::make_tuple("bool", "false", "int16", "0"),
                    std::make_tuple("bool", "false", "int32", "0"),
                    std::make_tuple("bool", "false", "int64", "0"),
                    std::make_tuple("bool", "false", "float", "0"),
                    std::make_tuple("bool", "false", "double", "0"),
                    std::make_tuple("bool", "false", "timestamp", "0"),
                    std::make_tuple("bool", "false", "string", "false"),
                    std::make_tuple("bool", "false", "string", "f"),
                    std::make_tuple("bool", "false", "string", "no"),
                    std::make_tuple("bool", "false", "string", "n"),
                    std::make_tuple("bool", "false", "string", "0"),
                    std::make_tuple("bool", "null", "bool", "null"),
                    std::make_tuple("bool", "null", "int16", "null"),
                    std::make_tuple("bool", "null", "int32", "null"),
                    std::make_tuple("bool", "null", "int64", "null"),
                    std::make_tuple("bool", "null", "float", "null"),
                    std::make_tuple("bool", "null", "double", "null"),
                    std::make_tuple("bool", "null", "date", "null"),
                    std::make_tuple("bool", "null", "timestamp", "null"),
                    std::make_tuple("bool", "null", "string", "null"),
                    std::make_tuple("bool", "null", "string", "abc"),
                    std::make_tuple("bool", "null", "string", "")));
// SafeCastNumber: bool, int16, int32
// UnSafeCst: int64, float, double
INSTANTIATE_TEST_SUITE_P(CastExprTestInt32, CastExprTest,
                         testing::ValuesIn(std::vector<std::tuple<std::string, std::string, std::string, std::string>>{
                             {"int32", "1", "bool", "true"},
                             {"int32", "1", "int16", "1"},
                             {"int32", "1", "int32", "1"},
                             {"int32", "1", "int64", "1"},
                             {"int32", "1", "float", "1"},
                             {"int32", "1", "double", "1"},
                             {"int32", "1", "string", "1"},
                             {"int32", "null", "date", "2020-10-12"},
                             {"int32", "977520480", "timestamp", "1590115420000"},
                             {"int32", "0", "bool", "false"},
                             {"int32", "-1", "int16", "-1"},
                             {"int32", "-1", "int32", "-1"},
                             {"int32", "-1", "int64", "-1"},
                             {"int32", "-1", "float", "-1"},
                             {"int32", "-1", "double", "-1"},
                             {"int32", "null", "bool", "null"},
                             {"int32", "null", "int16", "null"},
                             {"int32", "null", "int32", "null"},
                             {"int32", "null", "int64", "null"},
                             {"int32", "null", "float", "null"},
                             {"int32", "null", "double", "null"},
                             {"int32", "null", "string", "abc"},
                             {"int32", "null", "string", "null"},
                             {"int32", "null", "timestamp", "null"},

                             // numeric cast overflow: truncate
                             {"int32", "-1", "int64", "9223372036854775807"},
                             // int32(0x31993af1d7bffff) -> 0x1d7bffff
                             {"int32", "494665727", "int64", "223372036854775807"},
                             {"int32", "-494665727", "int64", "-223372036854775807"},
                             // string to numeric overflow: NULL
                             {"int32", "null", "string", "9223372036854775807"},
                             {"int32", "null", "string", "223372036854775807"},
                             {"int32", "null", "string", "-223372036854775807"},
                             {"int32", "12", "string", "12"},
                             {"int32", "12", "string", "012"},
                             {"int32", "0", "string", "0"},
                             {"int32", "9", "string", " 9"},
                             {"int32", "100", "string", " +100 "},
                             {"int32", "-999", "string", "-999\t"},
                             {"int32", "0", "string", "+0"},
                             {"int32", "0", "string", "-0"},
                             {"int32", "-1", "string", "-1"},
                             {"int32", "-88", "string", "-88"},
                             {"int32", "18", "string", "0x12"},
                             {"int32", "42", "string", "+0X2a"},
                             {"int32", "-43", "string", "-0X2b"},
                             {"int32", "-43", "string", "\t -0X2b"},
                             {"int32", "null", "string", "9223372036854775807"},
                             {"int32", "null", "string", "0x7FFFFFFFFFFFFFFF"},
                             {"int32", std::to_string(INT32_MAX), "string", "0x7FFFFFFF"},
                             {"int32", "null", "string", "-9223372036854775808"},
                             {"int32", "null", "string", ""},
                             {"int32", "null", "string", "-"},
                             {"int32", "null", "string", "+"},
                             {"int32", "null", "string", "8g"},
                             {"int32", "null", "string", "80x99"},
                             {"int32", "null", "string", "0x12k"},
                             {"int32", "null", "string", "8l"},
                             {"int32", "null", "string", "8 l"},
                             {"int32", "null", "string", "gg"},
                             {"int32", "null", "string", "89223372036854775807"},
                             {"int32", "null", "string", "-19223372036854775807"},
                         }));

// SafeCastNumber: bool, int16, int32, int64
// UnSafeCst: float, double
INSTANTIATE_TEST_SUITE_P(
    CastExprTestInt64, CastExprTest,
    testing::Values(
        std::make_tuple("int64", "1", "bool", "true"),
        std::make_tuple("int64", "1", "int16", "1"),
        std::make_tuple("int64", "1", "int32", "1"),
        std::make_tuple("int64", "1", "int64", "1"),
        std::make_tuple("int64", "1", "float", "1"),
        std::make_tuple("int64", "1", "double", "1"),
        std::make_tuple("int64", "null", "date", "2020-10-12"),
        std::make_tuple("int64", "1590115420000", "timestamp", "1590115420000"),
        std::make_tuple("int64", "1590115420000", "string", "1590115420000"),
        std::make_tuple("int64", "0", "bool", "false"),
        std::make_tuple("int64", "-1", "int16", "-1"),
        std::make_tuple("int64", "-1", "int32", "-1"),
        std::make_tuple("int64", "-1", "int64", "-1"),
        std::make_tuple("int64", "-1", "float", "-1"),
        std::make_tuple("int64", "-1", "double", "-1"),
        std::make_tuple("int64", "null", "bool", "null"),
        std::make_tuple("int64", "null", "int16", "null"),
        std::make_tuple("int64", "null", "int32", "null"),
        std::make_tuple("int64", "null", "int64", "null"),
        std::make_tuple("int64", "null", "float", "null"),
        std::make_tuple("int64", "null", "double", "null"),
        std::make_tuple("int64", "null", "string", "null"),
        std::make_tuple("int64", "null", "string", "abc"),
        std::make_tuple("int64", "null", "timestamp", "null")));
INSTANTIATE_TEST_SUITE_P(
    CastExprTestFloat, CastExprTest,
    testing::Values(std::make_tuple("float", "1", "bool", "true"),
                    std::make_tuple("float", "1", "int16", "1"),
                    std::make_tuple("float", "1", "int32", "1"),
                    std::make_tuple("float", "1", "int64", "1"),
                    std::make_tuple("float", "1", "float", "1"),
                    std::make_tuple("float", "1", "double", "1"),
                    std::make_tuple("float", "null", "date", "2020-10-12"),
                    std::make_tuple("float", "1", "timestamp", "1"),
                    std::make_tuple("float", "1", "string", "1"),
                    std::make_tuple("float", "0", "bool", "false"),
                    std::make_tuple("float", "-1", "int16", "-1"),
                    std::make_tuple("float", "-1", "int32", "-1"),
                    std::make_tuple("float", "-1", "int64", "-1"),
                    std::make_tuple("float", "-1", "float", "-1"),
                    std::make_tuple("float", "-1", "double", "-1"),
                    std::make_tuple("float", "null", "bool", "null"),
                    std::make_tuple("float", "null", "int16", "null"),
                    std::make_tuple("float", "null", "int32", "null"),
                    std::make_tuple("float", "null", "int64", "null"),
                    std::make_tuple("float", "null", "float", "null"),
                    std::make_tuple("float", "null", "double", "null"),
                    std::make_tuple("float", "null", "timestamp", "null")));

INSTANTIATE_TEST_SUITE_P(
    CastExprTestDouble, CastExprTest,
    testing::Values(
        //           C     A     B
        std::make_tuple("double", "1", "bool", "true"),
        std::make_tuple("double", "0", "bool", "false"),
        std::make_tuple("double", "1", "int16", "1"),
        std::make_tuple("double", "-1", "int16", "-1"),
        std::make_tuple("double", "1", "int32", "1"),
        std::make_tuple("double", "-1", "int32", "-1"),
        std::make_tuple("double", "1", "int64", "1"),
        std::make_tuple("double", "-1", "int64", "-1"),
        std::make_tuple("double", "1", "float", "1"),
        std::make_tuple("double", "-1", "float", "-1"),
        std::make_tuple("double", "1", "double", "1"),
        std::make_tuple("double", "null", "date", "2020-10-12"),
        std::make_tuple("double", "1590115420000.0", "timestamp",
                        "1590115420000"),
        std::make_tuple("double", "1590115420000.0", "string", "1590115420000"),
        std::make_tuple("double", "null", "bool", "null"),
        std::make_tuple("double", "null", "int16", "null"),
        std::make_tuple("double", "null", "int32", "null"),
        std::make_tuple("double", "null", "int64", "null"),
        std::make_tuple("double", "null", "float", "null"),
        std::make_tuple("double", "null", "double", "null"),
        std::make_tuple("double", "null", "string", "null"),
        std::make_tuple("double", "null", "string", "abc"),
        std::make_tuple("double", "null", "timestamp", "null")));

INSTANTIATE_TEST_SUITE_P(
    CastExprTestTimestamp, CastExprTest,
    testing::Values(std::make_tuple("timestamp", "1", "bool", "true"),
                    std::make_tuple("timestamp", "0", "bool", "false"),
                    std::make_tuple("timestamp", "1", "float", "1.0"),
                    std::make_tuple("timestamp", "1", "double", "1.0"),
                    std::make_tuple("timestamp", "1", "int16", "1"),
                    std::make_tuple("timestamp", "1", "int32", "1"),
                    std::make_tuple("timestamp", "1", "int64", "1"),
                    std::make_tuple("timestamp", "1590115420000", "string",
                                    "2020-05-22 10:43:40"),
                    std::make_tuple("timestamp", "1590151420000", "string",
                                    "2020-05-22 20:43:40"),
                    std::make_tuple("timestamp", "1589904000000", "date",
                                    "2020-05-20"),
                    std::make_tuple("timestamp", "null", "bool", "null"),
                    std::make_tuple("timestamp", "null", "int16", "null"),
                    std::make_tuple("timestamp", "null", "int32", "null"),
                    std::make_tuple("timestamp", "null", "int64", "null"),
                    std::make_tuple("timestamp", "null", "float", "null"),
                    std::make_tuple("timestamp", "null", "double", "null"),
                    std::make_tuple("timestamp", "null", "timestamp", "null"),
                    std::make_tuple("timestamp", "null", "date", "null"),
                    std::make_tuple("timestamp", "null", "string", "abc"),
                    std::make_tuple("timestamp", "null", "string", "null")));
INSTANTIATE_TEST_SUITE_P(
    CastExprTestDate, CastExprTest,
    testing::Values(
        std::make_tuple("date", "2020-05-22", "date", "2020-05-22"),
        std::make_tuple("date", "2020-05-22", "timestamp", "1590115420000"),
        std::make_tuple("date", "2020-05-22", "timestamp", "1590151420000"),
        std::make_tuple("date", "2020-05-22", "string", "2020-05-22 10:43:40"),
        std::make_tuple("date", "2020-05-22", "string", "2020-05-22 20:43:40"),
        std::make_tuple("date", "2020-05-22", "string", "2020-05-22"),
        std::make_tuple("date", "null", "date", "null"),
        std::make_tuple("date", "null", "timestamp", "null"),
        std::make_tuple("date", "null", "string", "abc"),
        std::make_tuple("date", "null", "string", "null")));

INSTANTIATE_TEST_SUITE_P(
    CastExprTestString, CastExprTest,
    testing::Values(std::make_tuple("string", "true", "bool", "true"),
                    std::make_tuple("string", "false", "bool", "false"),
                    std::make_tuple("string", "1", "int16", "1"),
                    std::make_tuple("string", "1", "int32", "1"),
                    std::make_tuple("string", "1", "int64", "1"),
                    std::make_tuple("string", "1", "float", "1"),
                    std::make_tuple("string", "1", "double", "1"),
                    std::make_tuple("string", "2020-05-22 10:43:40",
                                    "timestamp", "1590115420000"),
                    std::make_tuple("string", "2020-05-20", "date",
                                    "2020-05-20"),
                    std::make_tuple("string", "abc", "string", "abc"),
                    std::make_tuple("string", "null", "bool", "null"),
                    std::make_tuple("string", "null", "int16", "null"),
                    std::make_tuple("string", "null", "int32", "null"),
                    std::make_tuple("string", "null", "int64", "null"),
                    std::make_tuple("string", "null", "float", "null"),
                    std::make_tuple("string", "null", "double", "null"),
                    std::make_tuple("string", "null", "timestamp", "null"),
                    std::make_tuple("string", "null", "date", "null"),
                    std::make_tuple("string", "null", "string", "null")));

INSTANTIATE_TEST_SUITE_P(CastExprTestInt16, CastExprTest,
                         testing::ValuesIn(std::vector<std::tuple<std::string, std::string, std::string, std::string>>{
                             {"int16", "1", "bool", "true"},
                             {"int16", "1", "int16", "1"},
                             {"int16", "1", "int32", "1"},
                             {"int16", "1", "int64", "1"},
                             {"int16", "1", "float", "1"},
                             {"int16", "1", "double", "1"},
                             {"int16", "1", "string", "1"},
                             {"int16", "null", "date", "2020-10-12"},
                             {"int16", "-14496", "timestamp", "1590115420000"},
                             {"int16", "0", "bool", "false"},
                             {"int16", "-1", "int16", "-1"},
                             {"int16", "-1", "int32", "-1"},
                             {"int16", "-1", "int64", "-1"},
                             {"int16", "-1", "float", "-1"},
                             {"int16", "-1", "double", "-1"},
                             {"int16", "null", "bool", "null"},
                             {"int16", "null", "int16", "null"},
                             {"int16", "null", "int32", "null"},
                             {"int16", "null", "int64", "null"},
                             {"int16", "null", "float", "null"},
                             {"int16", "null", "double", "null"},
                             {"int16", "null", "string", "null"},
                             {"int16", "null", "string", "abc"},
                             {"int16", "null", "timestamp", "null"},
                             // int32 -> int16 overflow -> truncate value
                             {"int16", "-1", "int32", "6815743"},
                             {"int16", "1", "int32", "-6815743"},
                             {"int16", "28398", "int32", "487150"},
                         }));

TEST_P(CastExprTest, CastCheck) {
    auto [cast_type_str, cast_value_str, src_type_str, src_value_str] = GetParam();
    CastExprCheck(cast_type_str, cast_value_str, src_type_str, src_value_str);
}

class CastErrorExprTest
    : public ::testing::TestWithParam<std::tuple<std::string, std::string>> {
 public:
    CastErrorExprTest() { manager_ = new node::NodeManager(); }
    ~CastErrorExprTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};
template <typename CASTTYPE>
void CastErrorExprCheck(std::string src_type_str) {
    auto cast_func = [](node::NodeManager *nm, node::ExprNode *input) {
        return nm->MakeCastNode(
            DataTypeTrait<CASTTYPE>::to_type_node(nm)->base_, input);
    };

    hybridse::type::Type src_type;
    ASSERT_TRUE(hybridse::sqlcase::SqlCase::TypeParse(src_type_str, &src_type));
    switch (src_type) {
        case type::kBool: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>, udf::Nullable<bool>>(
                cast_func);
            break;
        }
        case type::kInt16: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>, udf::Nullable<int16_t>>(
                cast_func);
            break;
        }
        case type::kInt32: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>, udf::Nullable<int32_t>>(
                cast_func);
            break;
        }
        case type::kInt64: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>, udf::Nullable<int64_t>>(
                cast_func);
            break;
        }
        case type::kFloat: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>, udf::Nullable<float>>(
                cast_func);
            break;
        }
        case type::kDouble: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>, udf::Nullable<double>>(
                cast_func);
            break;
        }
        case type::kTimestamp: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>,
                           udf::Nullable<Timestamp>>(cast_func);
            break;
        }
        case type::kDate: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>, udf::Nullable<Date>>(
                cast_func);
            break;
        }
        case type::kVarchar: {
            ExprErrorCheck<udf::Nullable<CASTTYPE>,
                           udf::Nullable<StringRef>>(cast_func);
            break;
        }
        default: {
            FAIL() << "invalid casted from type "
                   << DataTypeTrait<CASTTYPE>::to_string() << " to type "
                   << type::Type_Name(src_type);
        }
    }
}

void CastErrorExprCheck(std::string cast_type_str, std::string src_type_str) {
    hybridse::type::Type cast_type;
    ASSERT_TRUE(
        hybridse::sqlcase::SqlCase::TypeParse(cast_type_str, &cast_type));
    switch (cast_type) {
        case type::kBool: {
            CastErrorExprCheck<bool>(src_type_str);
            break;
        }
        case type::kInt16: {
            CastErrorExprCheck<int16_t>(src_type_str);
            break;
        }
        case type::kInt32: {
            CastErrorExprCheck<int32_t>(src_type_str);
            break;
        }
        case type::kInt64: {
            CastErrorExprCheck<int64_t>(src_type_str);
            break;
        }
        case type::kFloat: {
            CastErrorExprCheck<float>(src_type_str);
            break;
        }
        case type::kDouble: {
            CastErrorExprCheck<double>(src_type_str);
            break;
        }
        case type::kTimestamp: {
            CastErrorExprCheck<Timestamp>(src_type_str);
            break;
        }
        case type::kDate: {
            CastErrorExprCheck<Date>(src_type_str);
            break;
        }
        case type::kVarchar: {
            CastErrorExprCheck<StringRef>(src_type_str);
            break;
        }
        default: {
            FAIL() << "invalid casted from type " << type::Type_Name(cast_type);
        }
    }
}
// TODO(chenjing): support timestamp -> date
INSTANTIATE_TEST_SUITE_P(
    CastExprErrorTestDate, CastErrorExprTest,
    testing::Values(
        // cast_type, src_type
        std::make_tuple("date", "bool"), std::make_tuple("date", "int16"),
        std::make_tuple("date", "int32"), std::make_tuple("date", "int64"),
        std::make_tuple("date", "float"), std::make_tuple("date", "double")));

TEST_P(CastErrorExprTest, CastErrorCheck) {
    auto [cast_type_str, src_type_str] = GetParam();
    CastErrorExprCheck(cast_type_str, src_type_str);
}

}  // namespace codegen
}  // namespace hybridse
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
