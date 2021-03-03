/*
 * default_udf_library.cc
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

/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * default_udf_library.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/default_udf_library.h"

#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "codegen/date_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "udf/containers.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"

using fesql::codec::Date;
using fesql::codec::StringRef;
using fesql::codec::Timestamp;
using fesql::codegen::CodeGenContext;
using fesql::codegen::NativeValue;
using fesql::common::kCodegenError;
using fesql::node::TypeNode;

namespace fesql {
namespace udf {

// static instance
DefaultUDFLibrary DefaultUDFLibrary::inst_;

template <typename T>
struct BuildGetHourUDF {
    using Args = std::tuple<T>;

    Status operator()(CodeGenContext* ctx, NativeValue time, NativeValue* out) {
        codegen::TimestampIRBuilder timestamp_ir_builder(ctx->GetModule());
        ::llvm::Value* ret = nullptr;
        Status status;
        CHECK_TRUE(timestamp_ir_builder.Hour(ctx->GetCurrentBlock(),
                                             time.GetRaw(), &ret, status),
                   kCodegenError,
                   "Fail to build udf hour(int64): ", status.str());
        *out = NativeValue::Create(ret);
        return status;
    }
};

template <typename T>
struct BuildGetMinuteUDF {
    using Args = std::tuple<T>;

    Status operator()(CodeGenContext* ctx, NativeValue time, NativeValue* out) {
        codegen::TimestampIRBuilder timestamp_ir_builder(ctx->GetModule());
        ::llvm::Value* ret = nullptr;
        Status status;
        CHECK_TRUE(timestamp_ir_builder.Minute(ctx->GetCurrentBlock(),
                                               time.GetRaw(), &ret, status),
                   kCodegenError,
                   "Fail to build udf hour(int64): ", status.str());
        *out = NativeValue::Create(ret);
        return status;
    }
};

template <typename T>
struct BuildGetSecondUDF {
    using Args = std::tuple<T>;

    Status operator()(CodeGenContext* ctx, NativeValue time, NativeValue* out) {
        codegen::TimestampIRBuilder timestamp_ir_builder(ctx->GetModule());
        ::llvm::Value* ret = nullptr;
        Status status;
        CHECK_TRUE(timestamp_ir_builder.Second(ctx->GetCurrentBlock(),
                                               time.GetRaw(), &ret, status),
                   kCodegenError,
                   "Fail to build udf hour(int64): ", status.str());
        *out = NativeValue::Create(ret);
        return status;
    }
};

template <typename T>
struct SumUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T>()
            .const_init(T(0))
            .update([](UDFResolveContext* ctx, ExprNode* cur_sum,
                       ExprNode* input) {
                auto nm = ctx->node_manager();
                auto is_null = nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
                auto new_sum =
                    nm->MakeBinaryExprNode(cur_sum, input, node::kFnOpAdd);
                return nm->MakeCondExpr(is_null, cur_sum, new_sum);
            })
            .output("identity");
    }
};

template <typename T>
struct MinUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, Tuple<bool, T>, T>()
            .const_init(MakeTuple(true, DataTypeTrait<T>::maximum_value()))
            .update([](UDFResolveContext* ctx, ExprNode* state,
                       ExprNode* input) {
                auto nm = ctx->node_manager();
                auto flag = nm->MakeGetFieldExpr(state, 0);
                auto cur_min = nm->MakeGetFieldExpr(state, 1);
                auto is_null = nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
                auto new_flag =
                    nm->MakeCondExpr(is_null, flag, nm->MakeConstNode(false));
                auto lt = nm->MakeBinaryExprNode(input, cur_min, node::kFnOpLt);
                auto new_min = nm->MakeCondExpr(lt, input, cur_min);
                new_min = nm->MakeCondExpr(is_null, cur_min, new_min);
                return nm->MakeFuncNode("make_tuple", {new_flag, new_min},
                                        nullptr);
            })
            .output([](UDFResolveContext* ctx, ExprNode* state) {
                auto nm = ctx->node_manager();
                auto flag = nm->MakeGetFieldExpr(state, 0);
                auto cur_min = nm->MakeGetFieldExpr(state, 1);
                return nm->MakeCondExpr(
                    flag,
                    nm->MakeCastNode(DataTypeTrait<T>::to_type_enum(),
                                     nm->MakeConstNode()),
                    cur_min);
            });
    }
};

template <>
struct MinUDAFDef<StringRef> {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<StringRef, Tuple<bool, StringRef>, StringRef>()
            .const_init(MakeTuple(true, StringRef("")))
            .update([](UDFResolveContext* ctx, ExprNode* state,
                       ExprNode* input) {
                auto nm = ctx->node_manager();
                auto flag = nm->MakeGetFieldExpr(state, 0);
                auto cur_min = nm->MakeGetFieldExpr(state, 1);
                auto is_null = nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
                auto new_flag =
                    nm->MakeCondExpr(is_null, flag, nm->MakeConstNode(false));
                auto lt = nm->MakeBinaryExprNode(input, cur_min, node::kFnOpLt);
                auto new_min = nm->MakeCondExpr(lt, input, cur_min);
                new_min = nm->MakeCondExpr(flag, input, new_min);
                new_min = nm->MakeCondExpr(is_null, cur_min, new_min);
                return nm->MakeFuncNode("make_tuple", {new_flag, new_min},
                                        nullptr);
            })
            .output([](UDFResolveContext* ctx, ExprNode* state) {
                auto nm = ctx->node_manager();
                auto flag = nm->MakeGetFieldExpr(state, 0);
                auto cur_min = nm->MakeGetFieldExpr(state, 1);
                return nm->MakeCondExpr(
                    flag, nm->MakeCastNode(node::kVarchar, nm->MakeConstNode()),
                    cur_min);
            });
    }
};

template <typename T>
struct MaxUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, Tuple<bool, T>, T>()
            .const_init(MakeTuple(true, DataTypeTrait<T>::minimum_value()))
            .update([](UDFResolveContext* ctx, ExprNode* state,
                       ExprNode* input) {
                auto nm = ctx->node_manager();
                auto flag = nm->MakeGetFieldExpr(state, 0);
                auto cur_max = nm->MakeGetFieldExpr(state, 1);
                auto is_null = nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
                auto new_flag =
                    nm->MakeCondExpr(is_null, flag, nm->MakeConstNode(false));
                auto gt = nm->MakeBinaryExprNode(input, cur_max, node::kFnOpGt);
                auto new_max = nm->MakeCondExpr(gt, input, cur_max);
                new_max = nm->MakeCondExpr(is_null, cur_max, new_max);
                return nm->MakeFuncNode("make_tuple", {new_flag, new_max},
                                        nullptr);
            })
            .output([](UDFResolveContext* ctx, ExprNode* state) {
                auto nm = ctx->node_manager();
                auto flag = nm->MakeGetFieldExpr(state, 0);
                auto cur_max = nm->MakeGetFieldExpr(state, 1);
                return nm->MakeCondExpr(
                    flag,
                    nm->MakeCastNode(DataTypeTrait<T>::to_type_enum(),
                                     nm->MakeConstNode()),
                    cur_max);
            });
    }
};

template <typename T>
struct CountUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<int64_t, int64_t, T>()
            .const_init(0)
            .update([](UDFResolveContext* ctx, ExprNode* cur_cnt,
                       ExprNode* input) {
                auto nm = ctx->node_manager();
                auto is_null = nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
                auto new_cnt = nm->MakeBinaryExprNode(
                    cur_cnt, nm->MakeConstNode(1), node::kFnOpAdd);
                return nm->MakeCondExpr(is_null, cur_cnt, new_cnt);
            })
            .output("identity");
    }
};

template <typename T>
struct AvgUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<double, Tuple<int64_t, double>, T>()
            .const_init(MakeTuple((int64_t)0, 0.0))
            .update(
                [](UDFResolveContext* ctx, ExprNode* state, ExprNode* input) {
                    auto nm = ctx->node_manager();
                    ExprNode* cnt = nm->MakeGetFieldExpr(state, 0);
                    ExprNode* sum = nm->MakeGetFieldExpr(state, 1);
                    ExprNode* is_null =
                        nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
                    cnt = nm->MakeBinaryExprNode(cnt, nm->MakeConstNode(1),
                                                 node::kFnOpAdd);
                    sum = nm->MakeBinaryExprNode(sum, input, node::kFnOpAdd);
                    auto new_state =
                        nm->MakeFuncNode("make_tuple", {cnt, sum}, nullptr);
                    return nm->MakeCondExpr(is_null, state, new_state);
                })
            .output([](UDFResolveContext* ctx, ExprNode* state) {
                auto nm = ctx->node_manager();
                ExprNode* cnt = nm->MakeGetFieldExpr(state, 0);
                ExprNode* sum = nm->MakeGetFieldExpr(state, 1);
                ExprNode* avg =
                    nm->MakeBinaryExprNode(sum, cnt, node::kFnOpFDiv);
                return avg;
            });
    }
};

template <typename T>
struct DistinctCountDef {
    using ArgT = typename DataTypeTrait<T>::CCallArgType;
    using SetT = std::unordered_set<T>;

    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        std::string suffix = ".opaque_std_set_" + DataTypeTrait<T>::to_string();
        helper.templates<int64_t, Opaque<SetT>, T>()
            .init("distinct_count_init" + suffix, init_set)
            .update("distinct_count_update" + suffix,
                    UpdateImpl<ArgT>::update_set)
            .output("distinct_count_output" + suffix, set_size);
    }

    static void init_set(SetT* addr) { new (addr) SetT(); }

    static int64_t set_size(SetT* set) {
        int64_t size = set->size();
        set->clear();
        set->~SetT();
        return size;
    }

    template <typename V>
    struct UpdateImpl {
        static SetT* update_set(SetT* set, V value) {
            set->insert(value);
            return set;
        }
    };

    template <typename V>
    struct UpdateImpl<V*> {
        static SetT* update_set(SetT* set, V* value) {
            set->insert(*value);
            return set;
        }
    };
};

template <typename T>
struct SumWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T, bool>()
            .const_init(0.0)
            .update([](UDFResolveContext* ctx, ExprNode* sum, ExprNode* elem,
                       ExprNode* cond) {
                auto nm = ctx->node_manager();
                if (elem->GetOutputType()->base() == node::kTimestamp) {
                    elem = nm->MakeCastNode(node::kInt64, elem);
                }
                auto new_sum =
                    nm->MakeBinaryExprNode(sum, elem, node::kFnOpAdd);
                ExprNode* update = nm->MakeCondExpr(cond, new_sum, sum);
                return update;
            })
            .output("identity");
    }
};

template <typename T>
struct CountWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<int64_t, int64_t, T, bool>()
            .const_init(0)
            .update([](UDFResolveContext* ctx, ExprNode* cnt, ExprNode* elem,
                       ExprNode* cond) {
                auto nm = ctx->node_manager();
                ExprNode* is_null =
                    nm->MakeUnaryExprNode(elem, node::kFnOpIsNull);
                ExprNode* new_cnt = nm->MakeBinaryExprNode(
                    cnt, nm->MakeConstNode(1), node::kFnOpAdd);
                new_cnt = nm->MakeCondExpr(is_null, cnt, new_cnt);
                ExprNode* update = nm->MakeCondExpr(cond, new_cnt, cnt);
                return update;
            })
            .output("identity");
    }
};

template <typename T>
struct AvgWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<double, Tuple<int64_t, double>, T, bool>()
            .const_init(MakeTuple((int64_t)0, 0.0))
            .update([](UDFResolveContext* ctx, ExprNode* state, ExprNode* elem,
                       ExprNode* cond) {
                auto nm = ctx->node_manager();
                ExprNode* cnt = nm->MakeGetFieldExpr(state, 0);
                ExprNode* sum = nm->MakeGetFieldExpr(state, 1);
                ExprNode* is_null =
                    nm->MakeUnaryExprNode(elem, node::kFnOpIsNull);

                ExprNode* new_cnt = nm->MakeBinaryExprNode(
                    cnt, nm->MakeConstNode(1), node::kFnOpAdd);
                new_cnt = nm->MakeCondExpr(is_null, cnt, new_cnt);

                if (elem->GetOutputType()->base() == node::kTimestamp) {
                    elem = nm->MakeCastNode(node::kInt64, elem);
                }

                ExprNode* new_sum =
                    nm->MakeBinaryExprNode(sum, elem, node::kFnOpAdd);
                new_sum = nm->MakeCondExpr(is_null, sum, new_sum);

                ExprNode* new_state =
                    nm->MakeFuncNode("make_tuple", {new_cnt, new_sum}, nullptr);
                return nm->MakeCondExpr(cond, new_state, state);
            })
            .output([](UDFResolveContext* ctx, ExprNode* state) {
                auto nm = ctx->node_manager();
                ExprNode* cnt = nm->MakeGetFieldExpr(state, 0);
                ExprNode* sum = nm->MakeGetFieldExpr(state, 1);
                ExprNode* avg =
                    nm->MakeBinaryExprNode(sum, cnt, node::kFnOpFDiv);
                return avg;
            });
    }
};

template <typename T>
struct MinWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T, bool>()
            .const_init(DataTypeTrait<T>::maximum_value())
            .update([](UDFResolveContext* ctx, ExprNode* min, ExprNode* elem,
                       ExprNode* cond) {
                auto nm = ctx->node_manager();
                if (elem->GetOutputType()->base() == node::kTimestamp) {
                    elem = nm->MakeCastNode(node::kInt64, elem);
                }
                auto condition =
                    nm->MakeBinaryExprNode(min, elem, node::kFnOpGt);
                ExprNode* new_min = nm->MakeCondExpr(condition, elem, min);
                ExprNode* update = nm->MakeCondExpr(cond, new_min, min);
                return update;
            })
            .output("identity");
    }
};

template <typename T>
struct MaxWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T, bool>()
            .const_init(DataTypeTrait<T>::minimum_value())
            .update([](UDFResolveContext* ctx, ExprNode* max, ExprNode* elem,
                       ExprNode* cond) {
                auto nm = ctx->node_manager();
                if (elem->GetOutputType()->base() == node::kTimestamp) {
                    elem = nm->MakeCastNode(node::kInt64, elem);
                }
                auto condition =
                    nm->MakeBinaryExprNode(max, elem, node::kFnOpLt);
                ExprNode* new_max = nm->MakeCondExpr(condition, elem, max);
                ExprNode* update = nm->MakeCondExpr(cond, new_max, max);
                return update;
            })
            .output("identity");
    }
};

template <typename T>
struct TopKDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        // register for i32 and i64 bound
        DoRegister<int32_t>(helper);
        DoRegister<int64_t>(helper);
    }

    template <typename BoundT>
    void DoRegister(UDAFRegistryHelper& helper) {  // NOLINT
        using ContainerT = udf::container::TopKContainer<T, BoundT>;
        std::string suffix = ".opaque_" + DataTypeTrait<BoundT>::to_string() +
                             "_bound_" + DataTypeTrait<T>::to_string();
        helper.templates<StringRef, Opaque<ContainerT>, Nullable<T>, BoundT>()
            .init("topk_init" + suffix, ContainerT::Init)
            .update("topk_update" + suffix, ContainerT::Push)
            .output("topk_output" + suffix, ContainerT::Output);
    }
};

void DefaultUDFLibrary::InitStringUDF() {
    RegisterExternalTemplate<v1::ToString>("string")
        .args_in<int16_t, int32_t, int64_t, float, double>()
        .return_by_arg(true);

    RegisterExternal("string")
        .args<bool>(static_cast<void (*)(bool, codec::StringRef*)>(
            udf::v1::bool_to_string))
        .return_by_arg(true);
    RegisterExternal("string")
        .args<Timestamp>(
            static_cast<void (*)(codec::Timestamp*, codec::StringRef*)>(
                udf::v1::timestamp_to_string))
        .return_by_arg(true);

    RegisterExternal("string")
        .args<Date>(static_cast<void (*)(codec::Date*, codec::StringRef*)>(
            udf::v1::date_to_string))
        .return_by_arg(true);

    RegisterCodeGenUDF("concat").variadic_args<>(
        /* infer */
        [](UDFResolveContext* ctx,
           const std::vector<const ExprAttrNode*>& arg_attrs,
           ExprAttrNode* out) {
            out->SetType(ctx->node_manager()->MakeTypeNode(node::kVarchar));
            out->SetNullable(false);
            return Status::OK();
        },
        /* gen */
        [](CodeGenContext* ctx, const std::vector<NativeValue>& args,
           NativeValue* out) {
            codegen::StringIRBuilder string_ir_builder(ctx->GetModule());
            return string_ir_builder.Concat(ctx->GetCurrentBlock(), args, out);
        });

    RegisterCodeGenUDF("concat_ws")
        .variadic_args<AnyArg>(
            /* infer */
            [](UDFResolveContext* ctx, const ExprAttrNode* arg,
               const std::vector<const ExprAttrNode*>& arg_types,
               ExprAttrNode* out) {
                out->SetType(ctx->node_manager()->MakeTypeNode(node::kVarchar));
                out->SetNullable(false);
                return Status::OK();
            },
            /* gen */
            [](CodeGenContext* ctx, NativeValue arg,
               const std::vector<NativeValue>& args, NativeValue* out) {
                codegen::StringIRBuilder string_ir_builder(ctx->GetModule());

                return string_ir_builder.ConcatWS(ctx->GetCurrentBlock(), arg,
                                                  args, out);
            });

    RegisterExternal("substring")
        .args<StringRef, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true)
        .doc(R"(
            Return a substring from string `str` starting at position `pos `.

            example:
            @code{.sql}

                select substr("hello world", 2);
                -- output "llo world"

            @endcode

            @param str
            @param pos define the begining of the substring.

            - If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
            - If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

            @since 2.0.0.0
            )");

    RegisterExternal("substring")
        .args<StringRef, int32_t, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true)
        .doc(R"(
            Return a substring `len` characters long from string str, starting at position `pos`.

            @code{.sql}

                select substr("hello world", 3, 6);
                -- output "llo wo"

            @endcode

            @param str
            @param pos: define the begining of the substring.

             - If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
             - If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

            @param len length of substring. If len is less than 1, the result is the empty string.

            @since 2.0.0.0
        )");

    RegisterAlias("substr", "substring");

    RegisterExternal("strcmp")
        .args<StringRef, StringRef>(
            static_cast<int32_t (*)(codec::StringRef*, codec::StringRef*)>(
                udf::v1::strcmp))
        .doc(R"(
            Returns 0 if the strings are the same, -1 if the first argument is smaller than the second according to the current sort order, and 1 otherwise.

            example
            @code{.sql}

                select strcmp("text", "text1");
                -- output -1
                select strcmp("text1", "text");
                -- output 1
                select strcmp("text", "text");
                -- output 0

            @endcode
            @since 2.0.0.0
        )");
    RegisterExternal("date_format")
        .args<Timestamp, StringRef>(
            static_cast<void (*)(codec::Timestamp*, codec::StringRef*,
                                 codec::StringRef*)>(udf::v1::date_format))
        .return_by_arg(true);
    RegisterExternal("date_format")
        .args<Date, StringRef>(
            static_cast<void (*)(codec::Date*, codec::StringRef*,
                                 codec::StringRef*)>(udf::v1::date_format))
        .return_by_arg(true);
}

void DefaultUDFLibrary::IniMathUDF() {
    RegisterExternal("log")
        .doc(R"(
            log(base, expr)
            If called with one parameter, this function returns the natural logarithm of expr.
            If called with two parameters, this function returns the logarithm of expr to the base.

            @code{.sql}

                SELECT LOG(1);  
                -- output 0.000000

                SELECT LOG(10,100);
                -- output 2
            @endcode

            @param base
            @param expr

            @since 2.0.0.0)")
        .args<float>(static_cast<float (*)(float)>(log))
        .args<double>(static_cast<double (*)(double)>(log));
    RegisterExprUDF("log").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("log do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("log", {cast}, nullptr);
        });
    RegisterExprUDF("log").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("log do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast1 = nm->MakeCastNode(node::kDouble, x);
            auto cast2 = nm->MakeCastNode(node::kDouble, y);
            auto logx = nm->MakeFuncNode("log", {cast1}, nullptr);
            auto logy = nm->MakeFuncNode("log", {cast2}, nullptr);
            return nm->MakeBinaryExprNode(logy, logx, node::kFnOpFDiv);
        });

    RegisterExternal("ln")
        .doc(R"(
            Return the natural logarithm of expr.

            @code{.sql}

                SELECT LN(1);  
                -- output 0.000000

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args<float>(static_cast<float (*)(float)>(log))
        .args<double>(static_cast<double (*)(double)>(log));
    RegisterExprUDF("ln").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("log do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("ln", {cast}, nullptr);
        });

    RegisterExternal("log2")
        .doc(R"(
            Return the base-2 logarithm of expr.

            @code{.sql}

                SELECT LOG2(65536);  
                -- output 16

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args<float>(static_cast<float (*)(float)>(log2))
        .args<double>(static_cast<double (*)(double)>(log2));
    RegisterExprUDF("log2").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("log2 do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("log2", {cast}, nullptr);
        });

    RegisterExternal("log10")
        .doc(R"(
            Return the base-10 logarithm of expr.

            @code{.sql}

                SELECT LOG10(100);  
                -- output 2

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args<float>(static_cast<float (*)(float)>(log10))
        .args<double>(static_cast<double (*)(double)>(log10));
    RegisterExprUDF("log10").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("log do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("log10", {cast}, nullptr);
        });

    RegisterExternalTemplate<v1::Abs>("abs")
        .doc(R"(
            Return the absolute value of expr.

            @code{.sql}

                SELECT ABS(-32);
                -- output 32

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int64_t, double>();
    RegisterExternalTemplate<v1::Abs32>("abs").args_in<int16_t, int32_t>();
    RegisterExprUDF("abs").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("abs do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("abs", {cast}, nullptr);
        });

    RegisterExternalTemplate<v1::Ceil>("ceil")
        .doc(R"(
            Return the smallest integer value not less than the expr

            @code{.sql}

                SELECT CEIL(1.23);
                -- output 2

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t>();
    RegisterExternal("ceil").args<double>(
        static_cast<double (*)(double)>(ceil));
    RegisterExprUDF("ceil").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("ceil do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("ceil", {cast}, nullptr);
        });

    RegisterAlias("ceiling", "ceil");

    RegisterExternalTemplate<v1::Exp>("exp")
        .doc(R"(
            Return the value of e (the base of natural logarithms) raised to the power of expr.

            @code{.sql}

                SELECT EXP(0);  
                -- output 1

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("exp").args<float>(static_cast<float (*)(float)>(expf));

    RegisterExternalTemplate<v1::Floor>("floor")
        .doc(R"(
            Return the largest integer value not less than the expr

            @code{.sql}

                SELECT FLOOR(1.23);
                -- output 1

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t>();
    RegisterExternal("floor").args<double>(
        static_cast<double (*)(double)>(floor));
    RegisterExprUDF("floor").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("floor do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("floor", {cast}, nullptr);
        });

    RegisterExternalTemplate<v1::Pow>("pow")
        .doc(R"(
            pow(expr1, expr2)
            Return the value of expr1 to the power of expr2.

            @code{.sql}

                SELECT POW(2, 10);
                -- output 1024.000000

            @endcode

            @param expr1
            @param expr2

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("pow").args<float, float>(
        static_cast<float (*)(float, float)>(powf));
    RegisterExprUDF("pow").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("pow do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            if (!y->GetOutputType()->IsArithmetic()) {
                ctx->SetError("pow do not support type " +
                              y->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast1 = nm->MakeCastNode(node::kDouble, x);
            auto cast2 = nm->MakeCastNode(node::kDouble, y);
            return nm->MakeFuncNode("pow", {cast1, cast2}, nullptr);
        });
    RegisterAlias("power", "pow");

    RegisterExternalTemplate<v1::Round>("round")
        .doc(R"(
            Return the nearest integer value to expr (in floating-point format), 
            rounding halfway cases away from zero, regardless of the current rounding mode.

            @code{.sql}

                SELECT ROUND(1.23);
                -- output 1

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int64_t, double>();
    RegisterExternalTemplate<v1::Round32>("round").args_in<int16_t, int32_t>();
    RegisterExprUDF("round").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("round do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("round", {cast}, nullptr);
        });

    RegisterExternalTemplate<v1::Sqrt>("sqrt")
        .doc(R"(
            Return square root of expr.

            @code{.sql}

                SELECT SQRT(100);
                -- output 10.000000

            @endcode

            @param expr: It is a single argument in radians.

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("sqrt").args<float>(static_cast<float (*)(float)>(sqrtf));

    RegisterExternalTemplate<v1::Truncate>("truncate")
        .doc(R"(
            Return the nearest integer that is not greater in magnitude than the expr.

            @code{.sql}

                SELECT TRUNCATE(1.23);
                -- output 1.0

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int64_t, double>();
    RegisterExternalTemplate<v1::Truncate32>("truncate")
        .args_in<int16_t, int32_t>();
    RegisterExprUDF("truncate")
        .args<AnyArg>([](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("truncate do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("truncate", {cast}, nullptr);
        });
}

void DefaultUDFLibrary::InitTrigonometricUDF() {
    RegisterExternalTemplate<v1::Acos>("acos")
        .doc(R"(
            Return the arc cosine of expr.

            @code{.sql}

                SELECT ACOS(1);
                -- output 0

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("acos").args<float>(static_cast<float (*)(float)>(acosf));

    RegisterExternalTemplate<v1::Asin>("asin")
        .doc(R"(
            Return the arc sine of expr.

            @code{.sql}

                SELECT ASIN(0.0);
                -- output 0.000000

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("asin").args<float>(static_cast<float (*)(float)>(asinf));

    RegisterExternalTemplate<v1::Atan>("atan")
        .doc(R"(
            atan(Y, X)
            If called with one parameter, this function returns the arc tangent of expr.
            If called with two parameters X and Y, this function returns the arc tangent of Y / X.

            @code{.sql}

                SELECT ATAN(-0.0);  
                -- output -0.000000

                SELECT ATAN(0, -0);
                -- output 3.141593

            @endcode

            @param X
            @param Y

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan").args<float>(static_cast<float (*)(float)>(atanf));

    RegisterExternalTemplate<v1::Atan2>("atan")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan").args<float, float>(
        static_cast<float (*)(float, float)>(atan2f));
    RegisterExprUDF("atan").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("atan do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            if (!y->GetOutputType()->IsArithmetic()) {
                ctx->SetError("atan do not support type " +
                              y->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast1 = nm->MakeCastNode(node::kDouble, x);
            auto cast2 = nm->MakeCastNode(node::kDouble, y);
            return nm->MakeFuncNode("atan", {cast1, cast2}, nullptr);
        });

    RegisterExternalTemplate<v1::Atan2>("atan2")
        .doc(R"(
            atan2(Y, X)
            Return the arc tangent of Y / X..

            @code{.sql}

                SELECT ATAN2(0, -0);
                -- output 3.141593

            @endcode

            @param X
            @param Y

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan2").args<float, float>(
        static_cast<float (*)(float, float)>(atan2f));
    RegisterExprUDF("atan2").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("atan2 do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            if (!y->GetOutputType()->IsArithmetic()) {
                ctx->SetError("atan2 do not support type " +
                              y->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast1 = nm->MakeCastNode(node::kDouble, x);
            auto cast2 = nm->MakeCastNode(node::kDouble, y);
            return nm->MakeFuncNode("atan2", {cast1, cast2}, nullptr);
        });

    RegisterExternalTemplate<v1::Cos>("cos")
        .doc(R"(
            Return the cosine of expr.

            @code{.sql}

                SELECT COS(0);
                -- output 1.000000

            @endcode

            @param expr: It is a single argument in radians.

            - The value returned by cos() is always in the range: -1 to 1.

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("cos").args<float>(static_cast<float (*)(float)>(cosf));

    RegisterExternalTemplate<v1::Cot>("cot")
        .doc(R"(
            Return the cotangent of expr.

            @code{.sql}

                SELECT COT(1);  
                -- output 0.6420926159343306

            @endcode

            @param expr

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("cot").args<float>(
        static_cast<float (*)(float)>(v1::Cotf));

    RegisterExternalTemplate<v1::Sin>("sin")
        .doc(R"(
            Return the sine of expr.

            @code{.sql}

                SELECT SIN(0);
                -- output 0.000000

            @endcode

            @param expr: It is a single argument in radians.

            - The value returned by sin() is always in the range: -1 to 1.

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("sin").args<float>(static_cast<float (*)(float)>(sinf));

    RegisterExternalTemplate<v1::Tan>("tan")
        .doc(R"(
            Return the tangent of expr.

            @code{.sql}

                SELECT TAN(0);
                -- output 0.000000

            @endcode

            @param expr: It is a single argument in radians.

            @since 2.0.0.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("tan").args<float>(static_cast<float (*)(float)>(tanf));
}

void DefaultUDFLibrary::InitUtilityUDF() {
    RegisterExprUDF("is_null")
        .args<AnyArg>([](UDFResolveContext* ctx, ExprNode* input) {
            return ctx->node_manager()->MakeUnaryExprNode(input,
                                                          node::kFnOpIsNull);
        })
        .doc(R"(
            Check if input value is null, return bool.
            @param input  Input value)");

    RegisterAlias("isnull", "is_null");

    RegisterExprUDF("if_null")
        .args<AnyArg, AnyArg>([](UDFResolveContext* ctx, ExprNode* input,
                                 ExprNode* default_val) {
            if (!node::TypeEquals(input->GetOutputType(),
                                  default_val->GetOutputType())) {
                ctx->SetError(
                    "Default value should take same type with input, expect " +
                    input->GetOutputType()->GetName() + " but get " +
                    default_val->GetOutputType()->GetName());
            }
            auto nm = ctx->node_manager();
            auto is_null = nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
            return nm->MakeCondExpr(
                is_null, default_val,
                nm->MakeUnaryExprNode(input, node::kFnOpNonNull));
        })
        .doc(R"(
            If input is not null, return input value; else return default value.
            @code{.sql}
                SELECT if_null("hello", "default"), if_null(NULL, "default");
                -- output ["hello", "default"]
            @endcode

            @param input    Input value
            @param default  Default value if input is null)");

    RegisterAlias("ifnull", "if_null");
}

void DefaultUDFLibrary::InitTypeUDF() {
    RegisterExternal("double")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, double*, bool*)>(
                v1::string_to_double)))
        .return_by_arg(true)
        .returns<Nullable<double>>();
    RegisterExternal("float")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, float*, bool*)>(
                v1::string_to_float)))
        .return_by_arg(true)
        .returns<Nullable<float>>();
    RegisterExternal("int32")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, int32_t*, bool*)>(
                v1::string_to_int)))
        .return_by_arg(true)
        .returns<Nullable<int32_t>>();
    RegisterExternal("int64")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, int64_t*, bool*)>(
                v1::string_to_bigint)))
        .return_by_arg(true)
        .returns<Nullable<int64_t>>();
    RegisterExternal("int16")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, int16_t*, bool*)>(
                v1::string_to_smallint)))
        .return_by_arg(true)
        .returns<Nullable<int16_t>>();
    RegisterExternal("bool")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, bool*, bool*)>(
                v1::string_to_bool)))
        .return_by_arg(true)
        .returns<Nullable<bool>>();

    RegisterExternal("date")
        .args<codec::Timestamp>(reinterpret_cast<void*>(
            static_cast<void (*)(Timestamp*, Date*, bool*)>(
                v1::timestamp_to_date)))
        .return_by_arg(true)
        .returns<Nullable<Date>>();
    RegisterExternal("date")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, Date*, bool*)>(
                v1::string_to_date)))
        .return_by_arg(true)
        .returns<Nullable<Date>>();
    RegisterExternal("timestamp")
        .args<codec::Date>(reinterpret_cast<void*>(
            static_cast<void (*)(Date*, Timestamp*, bool*)>(
                v1::date_to_timestamp)))
        .return_by_arg(true)
        .returns<Nullable<Timestamp>>();
    RegisterExternal("timestamp")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, Timestamp*, bool*)>(
                v1::string_to_timestamp)))
        .return_by_arg(true)
        .returns<Nullable<Timestamp>>();
}

void DefaultUDFLibrary::InitDateUDF() {
    RegisterExternal("year")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::year))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::year));

    RegisterCodeGenUDF("year")
        .args<Date>(
            [](CodeGenContext* ctx, NativeValue date, NativeValue* out) {
                codegen::DateIRBuilder date_ir_builder(ctx->GetModule());
                ::llvm::Value* ret = nullptr;
                Status status;
                CHECK_TRUE(date_ir_builder.Year(ctx->GetCurrentBlock(),
                                                date.GetRaw(), &ret, status),
                           kCodegenError,
                           "Fail to build udf year(date): ", status.str());
                *out = NativeValue::Create(ret);
                return status;
            })
        .returns<int32_t>();

    RegisterExternal("month")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::month))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::month));

    RegisterCodeGenUDF("month")
        .args<Date>(
            [](CodeGenContext* ctx, NativeValue date, NativeValue* out) {
                codegen::DateIRBuilder date_ir_builder(ctx->GetModule());
                ::llvm::Value* ret = nullptr;
                Status status;
                CHECK_TRUE(date_ir_builder.Month(ctx->GetCurrentBlock(),
                                                 date.GetRaw(), &ret, status),
                           kCodegenError,
                           "Fail to build udf month(date): ", status.str());
                *out = NativeValue::Create(ret);
                return status;
            })
        .returns<int32_t>();

    RegisterExternal("dayofmonth")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::dayofmonth))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::dayofmonth));

    RegisterCodeGenUDF("dayofmonth")
        .args<Date>(
            [](CodeGenContext* ctx, NativeValue date, NativeValue* out) {
                codegen::DateIRBuilder date_ir_builder(ctx->GetModule());
                ::llvm::Value* ret = nullptr;
                Status status;
                CHECK_TRUE(date_ir_builder.Day(ctx->GetCurrentBlock(),
                                               date.GetRaw(), &ret, status),
                           kCodegenError,
                           "Fail to build udf day(date): ", status.str());
                *out = NativeValue::Create(ret);
                return status;
            })
        .returns<int32_t>();

    RegisterAlias("day", "dayofmonth");

    RegisterExternal("dayofweek")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::dayofweek))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::dayofweek))
        .args<Date>(static_cast<int32_t (*)(Date*)>(v1::dayofweek));

    RegisterExternal("weekofyear")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::weekofyear))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::weekofyear))
        .args<Date>(static_cast<int32_t (*)(Date*)>(v1::weekofyear));

    RegisterAlias("week", "weekofyear");

    RegisterExternalTemplate<v1::IncOne>("inc")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterCodeGenUDFTemplate<BuildGetHourUDF>("hour")
        .args_in<int64_t, Timestamp>()
        .returns<int32_t>();

    RegisterCodeGenUDFTemplate<BuildGetMinuteUDF>("minute")
        .args_in<int64_t, Timestamp>()
        .returns<int32_t>();

    RegisterCodeGenUDFTemplate<BuildGetSecondUDF>("second")
        .args_in<int64_t, Timestamp>()
        .returns<int32_t>();

    RegisterExprUDF("identity")
        .args<AnyArg>([](UDFResolveContext* ctx, ExprNode* x) { return x; });

    RegisterExprUDF("add").args<AnyArg, AnyArg>([](UDFResolveContext* ctx,
                                                   ExprNode* x, ExprNode* y) {
        return ctx->node_manager()->MakeBinaryExprNode(x, y, node::kFnOpAdd);
    });

    RegisterCodeGenUDF("make_tuple")
        .variadic_args<>(
            /* infer */
            [](UDFResolveContext* ctx,
               const std::vector<const ExprAttrNode*>& args,
               ExprAttrNode* out) {
                auto nm = ctx->node_manager();
                auto tuple_type = nm->MakeTypeNode(node::kTuple);
                for (auto attr : args) {
                    tuple_type->generics_.push_back(attr->type());
                    tuple_type->generics_nullable_.push_back(attr->nullable());
                }
                out->SetType(tuple_type);
                out->SetNullable(false);
                return Status::OK();
            },
            /* gen */
            [](CodeGenContext* ctx, const std::vector<NativeValue>& args,
               NativeValue* out) {
                *out = NativeValue::CreateTuple(args);
                return Status::OK();
            });
}

void DefaultUDFLibrary::Init() {
    udf::RegisterNativeUDFToModule();
    InitUtilityUDF();
    InitDateUDF();
    InitTypeUDF();
    IniMathUDF();
    InitStringUDF();
    InitTrigonometricUDF();
    InitWindowFunctions();
    InitUDAF();
    InitFeatureZero();
}

void DefaultUDFLibrary::InitUDAF() {
    RegisterUDAFTemplate<SumUDAFDef>("sum")
        .doc("Compute sum of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp>();

    RegisterExprUDF("minimum").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
            auto nm = ctx->node_manager();
            auto cond = nm->MakeBinaryExprNode(x, y, node::kFnOpLt);
            return nm->MakeCondExpr(cond, x, y);
        });

    RegisterExprUDF("maximum").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
            auto nm = ctx->node_manager();
            auto cond = nm->MakeBinaryExprNode(x, y, node::kFnOpGt);
            return nm->MakeCondExpr(cond, x, y);
        });

    RegisterUDAFTemplate<MinUDAFDef>("min")
        .doc("Compute min of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUDAFTemplate<MaxUDAFDef>("max")
        .doc("Compute max of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUDAFTemplate<CountUDAFDef>("count")
        .doc("Compute count of values")
        .args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp,
                 Date, StringRef, LiteralTypedRow<>>();

    RegisterUDAFTemplate<AvgUDAFDef>("avg")
        .doc("Compute average of values")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUDAFTemplate<DistinctCountDef>("distinct_count")
        .doc("Compute distinct number of values")
        .args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp,
                 Date, StringRef>();

    RegisterUDAFTemplate<SumWhereDef>("sum_where")
        .doc("Compute sum of values match specified condition")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUDAFTemplate<CountWhereDef>("count_where")
        .doc("Compute number of values match specified condition")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUDAFTemplate<AvgWhereDef>("avg_where")
        .doc("Compute average of values match specified condition")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUDAFTemplate<MinWhereDef>("min_where")
        .doc("Compute minimum of values match specified condition")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUDAFTemplate<MaxWhereDef>("max_where")
        .doc("Compute maximum of values match specified condition")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUDAFTemplate<TopKDef>("top")
        .doc(
            "Compute top k of values and output string separated by comma. "
            "The outputs are sorted in desc order")
        .args_in<int16_t, int32_t, int64_t, float, double, Date, Timestamp,
                 StringRef>();

    InitAggByCateUDAFs();
}

}  // namespace udf
}  // namespace fesql
