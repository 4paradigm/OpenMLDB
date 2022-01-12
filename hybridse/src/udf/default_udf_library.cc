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

#include "udf/default_udf_library.h"

#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "codegen/date_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "udf/containers.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"

using hybridse::codec::Date;
using hybridse::codec::StringRef;
using hybridse::codec::Timestamp;
using hybridse::codegen::CodeGenContext;
using hybridse::codegen::NativeValue;
using hybridse::common::kCodegenError;

namespace hybridse {
namespace udf {

// static instance
DefaultUdfLibrary DefaultUdfLibrary::inst_;

template <typename T>
struct BuildGetHourUdf {
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
struct BuildGetMinuteUdf {
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
struct BuildGetSecondUdf {
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
struct SumUdafDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T>()
            .const_init(T(0))
            .update([](UdfResolveContext* ctx, ExprNode* cur_sum,
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
struct MinUdafDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<T, Tuple<bool, T>, T>()
            .const_init(MakeTuple(true, DataTypeTrait<T>::maximum_value()))
            .update([](UdfResolveContext* ctx, ExprNode* state,
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
            .output([](UdfResolveContext* ctx, ExprNode* state) {
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
struct MinUdafDef<StringRef> {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<StringRef, Tuple<bool, StringRef>, StringRef>()
            .const_init(MakeTuple(true, StringRef("")))
            .update([](UdfResolveContext* ctx, ExprNode* state,
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
            .output([](UdfResolveContext* ctx, ExprNode* state) {
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
struct MaxUdafDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<T, Tuple<bool, T>, T>()
            .const_init(MakeTuple(true, DataTypeTrait<T>::minimum_value()))
            .update([](UdfResolveContext* ctx, ExprNode* state,
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
            .output([](UdfResolveContext* ctx, ExprNode* state) {
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
struct CountUdafDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<int64_t, int64_t, T>()
            .const_init(0)
            .update([](UdfResolveContext* ctx, ExprNode* cur_cnt,
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
struct AvgUdafDef {
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<double, Tuple<int64_t, double>, T>()
            .const_init(MakeTuple(static_cast<int64_t>(0), 0.0))
            .update(
                [](UdfResolveContext* ctx, ExprNode* state, ExprNode* input) {
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
            .output([](UdfResolveContext* ctx, ExprNode* state) {
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

    void operator()(UdafRegistryHelper& helper) {  // NOLINT
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
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T, bool>()
            .const_init(0.0)
            .update([](UdfResolveContext* ctx, ExprNode* sum, ExprNode* elem,
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
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<int64_t, int64_t, T, bool>()
            .const_init(0)
            .update([](UdfResolveContext* ctx, ExprNode* cnt, ExprNode* elem,
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
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<double, Tuple<int64_t, double>, T, bool>()
            .const_init(MakeTuple(static_cast<int64_t>(0), 0.0))
            .update([](UdfResolveContext* ctx, ExprNode* state, ExprNode* elem,
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
            .output([](UdfResolveContext* ctx, ExprNode* state) {
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
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T, bool>()
            .const_init(DataTypeTrait<T>::maximum_value())
            .update([](UdfResolveContext* ctx, ExprNode* min, ExprNode* elem,
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
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T, bool>()
            .const_init(DataTypeTrait<T>::minimum_value())
            .update([](UdfResolveContext* ctx, ExprNode* max, ExprNode* elem,
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
    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        // register for i32 and i64 bound
        DoRegister<int32_t>(helper);
        DoRegister<int64_t>(helper);
    }

    template <typename BoundT>
    void DoRegister(UdafRegistryHelper& helper) {  // NOLINT
        using ContainerT = udf::container::TopKContainer<T, BoundT>;
        std::string suffix = ".opaque_" + DataTypeTrait<BoundT>::to_string() +
                             "_bound_" + DataTypeTrait<T>::to_string();
        helper.templates<StringRef, Opaque<ContainerT>, Nullable<T>, BoundT>()
            .init("topk_init" + suffix, ContainerT::Init)
            .update("topk_update" + suffix, ContainerT::Push)
            .output("topk_output" + suffix, ContainerT::Output);
    }
};

void DefaultUdfLibrary::InitStringUdf() {
    RegisterExternalTemplate<v1::ToString>("string")
        .args_in<int16_t, int32_t, int64_t, float, double>()
        .return_by_arg(true)
        .doc(R"(
            @brief Return string converted from numeric expression

            Example:

            @code{.sql}
                select string(123);
                -- output "123"

                select string(1.23);
                -- output "1.23"
            @endcode
            @since 0.1.0)");

    RegisterExternal("string")
        .args<bool>(static_cast<void (*)(bool, codec::StringRef*)>(
                        udf::v1::bool_to_string))
        .return_by_arg(true)
        .doc(R"(
            @brief Return string converted from bool expression

            Example:

            @code{.sql}
                select string(true);
                -- output "true"

                select string(false);
                -- output "false"
            @endcode
            @since 0.1.0)");
    RegisterExternal("string")
        .args<Timestamp>(
            static_cast<void (*)(codec::Timestamp*, codec::StringRef*)>(
                udf::v1::timestamp_to_string))
        .return_by_arg(true)
        .doc(R"(
            @brief Return string converted from timestamp expression

            Example:

            @code{.sql}
                select string(timestamp(1590115420000));
                -- output "2020-05-22 10:43:40"
            @endcode
            @since 0.1.0)");

    RegisterExternal("string")
        .args<Date>(static_cast<void (*)(codec::Date*, codec::StringRef*)>(
                        udf::v1::date_to_string))
        .return_by_arg(true)
        .doc(R"(
            @brief Return string converted from timestamp expression

            Example:

            @code{.sql}
                select string(timestamp(1590115420000));
                -- output "2020-05-22 10:43:40"
            @endcode
            @since 0.1.0)");

    RegisterCodeGenUdf("concat").variadic_args<>(
        /* infer */
        [](UdfResolveContext* ctx,
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
        })
        .doc(R"(
            @brief This function returns a string resulting from the joining of two or more string values in an end-to-end manner.
            (To add a separating value during joining, see concat_ws.)

            Example:

            @code{.sql}
                select concat("1", 2, 3, 4, 5.6, 7.8, Timestamp(1590115420000L));
                -- output "12345.67.82020-05-22 10:43:40"
            @endcode
            @since 0.1.0)");

    RegisterCodeGenUdf("concat_ws")
        .variadic_args<AnyArg>(
            /* infer */
            [](UdfResolveContext* ctx, const ExprAttrNode* arg,
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
            })
            .doc(R"(
                @brief Returns a string resulting from the joining of two or more string value in an end-to-end manner.
                It separates those concatenated string values with the delimiter specified in the first function argument.

                Example:

                @code{.sql}
                    select concat("-", "1", 2, 3, 4, 5.6, 7.8, Timestamp(1590115420000L));
                    -- output "1-2-3-4-5.6-7.8-2020-05-22 10:43:40"
                @endcode
                @since 0.1.0)");

    RegisterExternal("substring")
        .args<StringRef, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true)
        .doc(R"(
            @brief Return a substring from string `str` starting at position `pos `.

            Note: This function equals the `substr()` function.

            Example:

            @code{.sql}

                select substr("hello world", 2);
                -- output "llo world"

                select substring("hello world", 2);
                -- output "llo world"
            @endcode

            @param str
            @param pos define the begining of the substring.

            - If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
            - If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

            @since 0.1.0)");

    RegisterExternal("substring")
        .args<StringRef, int32_t, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true)
        .doc(R"(
            @brief Return a substring `len` characters long from string str, starting at position `pos`.
            Alias function: `substr`

            Example:

            @code{.sql}

                select substr("hello world", 3, 6);
                -- output "llo wo"

            @endcode

            @param str
            @param pos: define the begining of the substring.

             - If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
             - If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

            @param len length of substring. If len is less than 1, the result is the empty string.

            @since 0.1.0)");

    RegisterAlias("substr", "substring");

    RegisterExternal("strcmp")
        .args<StringRef, StringRef>(
            static_cast<int32_t (*)(codec::StringRef*, codec::StringRef*)>(
                udf::v1::strcmp))
        .doc(R"(
            @brief Returns 0 if the strings are the same, -1 if the first argument is smaller than the second according to the current sort order, and 1 otherwise.

            Example:

            @code{.sql}

                select strcmp("text", "text1");
                -- output -1
                select strcmp("text1", "text");
                -- output 1
                select strcmp("text", "text");
                -- output 0

            @endcode

            @since 0.1.0)");
    RegisterExternal("date_format")
        .args<Timestamp, StringRef>(
            static_cast<void (*)(codec::Timestamp*, codec::StringRef*,
                                 codec::StringRef*)>(udf::v1::date_format))
        .return_by_arg(true)
        .doc(R"(
            @brief Formats the datetime value according to the format string.

            Example:

            @code{.sql}
                select date_format(timestamp(1590115420000),"%Y-%m-%d %H:%M:%S");
                --output "2020-05-22 10:43:40"
            @endcode)");
    RegisterExternal("date_format")
        .args<Date, StringRef>(
            static_cast<void (*)(codec::Date*, codec::StringRef*,
                                 codec::StringRef*)>(udf::v1::date_format))
        .return_by_arg(true)
        .doc(R"(
            @brief Formats the date value according to the format string.

            Example:

            @code{.sql}
                select date_format(date(1590115420000),"%Y-%m-%d");
                --output "2020-05-22"
            @endcode)");
    /// Escape is Nullable
    /// if escape is null, we will deal with it. Regarding it as an empty string. See more details in udf::v1::ilike
    RegisterExternal("like_match")
        .args<StringRef, StringRef, StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(codec::StringRef*, codec::StringRef*, codec::StringRef*, bool*, bool*)>(
                udf::v1::like)))
        .return_by_arg(true)
        .returns<Nullable<bool>>()
        .doc(R"r(
                @brief pattern match same as LIKE predicate

                Rules:
                1. Special characters:
                   - underscore(_): exact one character
                   - precent(%): zero or more characters.
                2. Escape character:
                   - backslash(\) is the default escape character
                   - length of <escape character> must <= 1
                   - if <escape character> is empty, escape feature is disabled
                3. case sensitive
                4. backslash: sql string literal use backslash(\) for escape sequences, write '\\' as backslash itself
                5. if one or more of target, pattern and escape are null values, then the result is null

                Example:
                @code{.sql}
                    select like_match('Mike', 'Mi_e', '\\')
                    -- output: true

                    select like_match('Mike', 'Mi\\_e', '\\')
                    -- output: false

                    select like_match('Mi_e', 'Mi\\_e', '\\')
                    -- output: true

                    select like_match('Mi\\ke', 'Mi\\_e', '')
                    -- output: true

                    select like_match('Mi\\ke', 'Mi\\_e', string(null))
                    -- output: null
                @endcode

                @param target: string to match

                @param pattern: the glob match pattern

                @param escape: escape character

                @since 0.4.0
        )r");
    RegisterExternal("like_match")
        .args<StringRef, StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(codec::StringRef*, codec::StringRef*, bool*, bool*)>(udf::v1::like)))
        .return_by_arg(true)
        .returns<Nullable<bool>>()
        .doc(R"r(
                @brief pattern match same as LIKE predicate

                Rules:
                1. Special characters:
                   - underscore(_): exact one character
                   - precent(%): zero or more characters.
                2. Escape character is backslash(\) by default
                3. case sensitive
                4. backslash: sql string literal use backslash(\) for escape sequences, write '\\' as backslash itself
                5. if one or more of target, pattern then the result is null

                Example:
                @code{.sql}
                    select like_match('Mike', 'Mi_k')
                    -- output: true
                    select like_match('Mike', 'mi_k')
                    -- output: false
                @endcode

                @param target: string to match

                @param pattern: the glob match pattern

                @since 0.4.0
        )r");
    /// Escape is Nullable
    /// if escape is null, we will deal with it. Regarding it as an empty string. See more details in udf::v1::ilike
    RegisterExternal("ilike_match")
        .args<StringRef, StringRef, StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(codec::StringRef*, codec::StringRef*, codec::StringRef*, bool*, bool*)>(
                udf::v1::ilike)))
        .return_by_arg(true)
        .returns<Nullable<bool>>()
        .doc(R"r(
                @brief pattern match same as ILIKE predicate

                Rules:
                1. Special characters:
                   - underscore(_): exact one character
                   - precent(%): zero or more characters.
                2. Escape character:
                   - backslash(\) is the default escape character
                   - length of <escape character> must <= 1
                   - if <escape character> is empty, escape feautre is disabled
                3. case insensitive
                4. backslash: sql string literal use backslash(\) for escape sequences, write '\\' as backslash itself
                5. if one or more of target, pattern and escape are null values, then the result is null


                Example:
                @code{.sql}
                    select ilike_match('Mike', 'mi_e', '\\')
                    -- output: true

                    select ilike_match('Mike', 'mi\\_e', '\\')
                    -- output: false

                    select ilike_match('Mi_e', 'mi\\_e', '\\')
                    -- output: true

                    select ilike_match('Mi\\ke', 'mi\\_e', '')
                    -- output: true

                    select ilike_match('Mi\\ke', 'mi\\_e', string(null))
                    -- output: null
                @endcode

                @param target: string to match

                @param pattern: the glob match pattern

                @param escape: escape character

                @since 0.4.0
        )r");
    RegisterExternal("ilike_match")
        .args<StringRef, StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(codec::StringRef*, codec::StringRef*, bool*, bool*)>(udf::v1::ilike)))
        .return_by_arg(true)
        .returns<Nullable<bool>>()
        .doc(R"r(
                @brief pattern match same as ILIKE predicate

                Rules:
                1. Special characters:
                   - underscore(_): exact one character
                   - precent(%): zero or more characters.
                2. Escape character: backslash(\) is the default escape character
                3. case insensitive
                4. backslash: sql string literal use backslash(\) for escape sequences, write '\\' as backslash itself
                5. Return NULL if target or pattern is NULL

                Example:
                @code{.sql}
                    select ilike_match('Mike', 'Mi_k')
                    -- output: true

                    select ilike_match('Mike', 'mi_k')
                    -- output: true
                @endcode

                @param target: string to match

                @param pattern: the glob match pattern

                @since 0.4.0
        )r");
    RegisterExternal("ucase")
        .args<codec::StringRef>(
            reinterpret_cast<void*>(static_cast<void (*)(codec::StringRef*, codec::StringRef*, bool*)>(udf::v1::ucase)))
        .return_by_arg(true)
        .returns<Nullable<codec::StringRef>>()
        .doc(R"(
            @brief Convert all the characters to uppercase. Note that characters values > 127 are simply returned.

            Example:

            @code{.sql}
                SELECT UCASE('Sql') as str1;
                --output "SQL"
            @endcode
            @since 0.4.0)");
    RegisterExternal("reverse")
        .args<codec::StringRef>(
            reinterpret_cast<void*>(static_cast<void (*)(codec::StringRef*, codec::StringRef*, bool*)>(udf::v1::reverse)))
        .return_by_arg(true)
        .returns<Nullable<codec::StringRef>>()
        .doc(R"(
            @brief Returns the reversed given string.

            Example:

            @code{.sql}
                SELECT REVERSE('abc') as str1;
                --output "cba"
            @endcode
            @since 0.4.0)");
    RegisterAlias("upper", "ucase");
}

void DefaultUdfLibrary::InitMathUdf() {
    RegisterExternal("log")
        .doc(R"(
            @brief log(base, expr)
            If called with one parameter, this function returns the natural logarithm of expr.
            If called with two parameters, this function returns the logarithm of expr to the base.

            Example:

            @code{.sql}

                SELECT LOG(1);  
                -- output 0.000000

                SELECT LOG(10,100);
                -- output 2
            @endcode

            @param base

            @param expr

            @since 0.1.0)")
        .args<float>(static_cast<float (*)(float)>(log))
        .args<double>(static_cast<double (*)(double)>(log));
    RegisterExprUdf("log").args<AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("log do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("log", {cast}, nullptr);
        });
    RegisterExprUdf("log").args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) -> ExprNode* {
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
            @brief Return the natural logarithm of expr.

            Example:

            @code{.sql}

                SELECT LN(1);  
                -- output 0.000000

            @endcode

            @param expr

            @since 0.1.0)")
        .args<float>(static_cast<float (*)(float)>(log))
        .args<double>(static_cast<double (*)(double)>(log));
    RegisterExprUdf("ln").args<AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
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
            @brief Return the base-2 logarithm of expr.

            Example:

            @code{.sql}

                SELECT LOG2(65536);  
                -- output 16

            @endcode

            @param expr

            @since 0.1.0)")
        .args<float>(static_cast<float (*)(float)>(log2))
        .args<double>(static_cast<double (*)(double)>(log2));
    RegisterExprUdf("log2").args<AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
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
            @brief Return the base-10 logarithm of expr.

            Example:

            @code{.sql}

                SELECT LOG10(100);  
                -- output 2

            @endcode

            @param expr

            @since 0.1.0)")
        .args<float>(static_cast<float (*)(float)>(log10))
        .args<double>(static_cast<double (*)(double)>(log10));
    RegisterExprUdf("log10").args<AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
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
            @brief Return the absolute value of expr.

            Example:

            @code{.sql}

                SELECT ABS(-32);
                -- output 32

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int64_t, double>();
    RegisterExternalTemplate<v1::Abs32>("abs").args_in<int16_t, int32_t>();
    RegisterExprUdf("abs").args<AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
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
            @brief Return the smallest integer value not less than the expr

            Example:

            @code{.sql}

                SELECT CEIL(1.23);
                -- output 2

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t>();
    RegisterExternal("ceil").args<double>(
        static_cast<double (*)(double)>(ceil));
    RegisterExprUdf("ceil").args<AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
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
            @brief Return the value of e (the base of natural logarithms) raised to the power of expr.

            @code{.sql}

                SELECT EXP(0);  
                -- output 1

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("exp").args<float>(static_cast<float (*)(float)>(expf));

    RegisterExternalTemplate<v1::Floor>("floor")
        .doc(R"(
            @brief Return the largest integer value not less than the expr

            Example:

            @code{.sql}

                SELECT FLOOR(1.23);
                -- output 1

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t>();
    RegisterExternal("floor").args<double>(
        static_cast<double (*)(double)>(floor));
    RegisterExprUdf("floor").args<AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
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
            @brief Return the value of expr1 to the power of expr2.

            Example:

            @code{.sql}

                SELECT POW(2, 10);
                -- output 1024.000000

            @endcode

            @param expr1
            @param expr2

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("pow").args<float, float>(
        static_cast<float (*)(float, float)>(powf));
    RegisterExprUdf("pow").args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) -> ExprNode* {
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
            @brief Return the nearest integer value to expr (in floating-point format),
            rounding halfway cases away from zero, regardless of the current rounding mode.

            Example:

            @code{.sql}

                SELECT ROUND(1.23);
                -- output 1

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int64_t, double>();
    RegisterExternalTemplate<v1::Round32>("round").args_in<int16_t, int32_t>();
    RegisterExprUdf("round").args<AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
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
            @brief Return square root of expr.

            Example:

            @code{.sql}

                SELECT SQRT(100);
                -- output 10.000000

            @endcode

            @param expr: It is a single argument in radians.

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("sqrt").args<float>(static_cast<float (*)(float)>(sqrtf));

    RegisterExternalTemplate<v1::Truncate>("truncate")
        .doc(R"(
            @brief Return the nearest integer that is not greater in magnitude than the expr.

            Example:

            @code{.sql}

                SELECT TRUNCATE(1.23);
                -- output 1.0

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int64_t, double>();
    RegisterExternalTemplate<v1::Truncate32>("truncate")
        .args_in<int16_t, int32_t>();
    RegisterExprUdf("truncate")
        .args<AnyArg>([](UdfResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("truncate do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("truncate", {cast}, nullptr);
        });
    InitTrigonometricUdf();
}

void DefaultUdfLibrary::InitTrigonometricUdf() {
    RegisterExternalTemplate<v1::Acos>("acos")
        .doc(R"(
            @brief Return the arc cosine of expr.

            Example:

            @code{.sql}

                SELECT ACOS(1);
                -- output 0

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("acos").args<float>(static_cast<float (*)(float)>(acosf));

    RegisterExternalTemplate<v1::Asin>("asin")
        .doc(R"(
            @brief Return the arc sine of expr.

            Example:

            @code{.sql}

                SELECT ASIN(0.0);
                -- output 0.000000

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("asin").args<float>(static_cast<float (*)(float)>(asinf));

    RegisterExternalTemplate<v1::Atan>("atan")
        .doc(R"(
            @brief Return the arc tangent of expr
            If called with one parameter, this function returns the arc tangent of expr.
            If called with two parameters X and Y, this function returns the arc tangent of Y / X.

            Example:

            @code{.sql}

                SELECT ATAN(-0.0);  
                -- output -0.000000

                SELECT ATAN(0, -0);
                -- output 3.141593

            @endcode

            @param X
            @param Y

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan").args<float>(static_cast<float (*)(float)>(atanf));

    RegisterExternalTemplate<v1::Atan2>("atan")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan").args<float, float>(
        static_cast<float (*)(float, float)>(atan2f));
    RegisterExprUdf("atan").args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) -> ExprNode* {
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
            @brief Return the arc tangent of Y / X..

            Example:

            @code{.sql}

                SELECT ATAN2(0, -0);
                -- output 3.141593

            @endcode

            @param X
            @param Y

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan2").args<float, float>(
        static_cast<float (*)(float, float)>(atan2f));
    RegisterExprUdf("atan2").args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) -> ExprNode* {
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
            @brief Return the cosine of expr.

            Example:

            @code{.sql}

                SELECT COS(0);
                -- output 1.000000

            @endcode

            @param expr: It is a single argument in radians.

            - The value returned by cos() is always in the range: -1 to 1.

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("cos").args<float>(static_cast<float (*)(float)>(cosf));

    RegisterExternalTemplate<v1::Cot>("cot")
        .doc(R"(
            @brief Return the cotangent of expr.

            Example:

            @code{.sql}

                SELECT COT(1);  
                -- output 0.6420926159343306

            @endcode

            @param expr

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("cot").args<float>(
        static_cast<float (*)(float)>(v1::Cotf));

    RegisterExternalTemplate<v1::Sin>("sin")
        .doc(R"(
            @brief Return the sine of expr.

            Example:

            @code{.sql}

                SELECT SIN(0);
                -- output 0.000000

            @endcode

            @param expr: It is a single argument in radians.

            - The value returned by sin() is always in the range: -1 to 1.

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("sin").args<float>(static_cast<float (*)(float)>(sinf));

    RegisterExternalTemplate<v1::Tan>("tan")
        .doc(R"(
            @brief Return the tangent of expr.

            Example:

            @code{.sql}

                SELECT TAN(0);
                -- output 0.000000

            @endcode

            @param expr: It is a single argument in radians.

            @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("tan").args<float>(static_cast<float (*)(float)>(tanf));
}

void DefaultUdfLibrary::InitLogicalUdf() {
    RegisterExprUdf("is_null")
        .args<AnyArg>([](UdfResolveContext* ctx, ExprNode* input) {
            return ctx->node_manager()->MakeUnaryExprNode(input,
                                                          node::kFnOpIsNull);
        })
        .doc(R"(
            @brief  Check if input value is null, return bool.

            @param input  Input value

            @since 0.1.0)");

    RegisterAlias("isnull", "is_null");

    RegisterExprUdf("if_null")
        .args<AnyArg, AnyArg>([](UdfResolveContext* ctx, ExprNode* input,
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
            @brief If input is not null, return input value; else return default value.

            Example:

            @code{.sql}
                SELECT if_null("hello", "default"), if_null(NULL, "default");
                -- output ["hello", "default"]
            @endcode

            @param input    Input value
            @param default  Default value if input is null

            @since 0.1.0)");

    RegisterAlias("ifnull", "if_null");
    RegisterAlias("nvl", "if_null");
    RegisterExprUdf("nvl2")
        .args<AnyArg, AnyArg, AnyArg>([](UdfResolveContext* ctx, ExprNode* expr1, ExprNode* expr2, ExprNode* expr3) {
            if (!node::TypeEquals(expr2->GetOutputType(), expr3->GetOutputType())) {
                ctx->SetError(absl::StrCat("expr3 should take same type with expr2, expect ",
                                           expr2->GetOutputType()->GetName(), " but get ",
                                           expr3->GetOutputType()->GetName()));
            }
            auto nm = ctx->node_manager();
            return nm->MakeCondExpr(nm->MakeUnaryExprNode(expr1, node::kFnOpIsNull), expr3, expr2);
        })
        .doc(R"(
        @brief nvl2(expr1, expr2, expr3) - Returns expr2 if expr1 is not null, or expr3 otherwise.

        Example:

        @code{.sql}
            SELECT nvl2(NULL, 2, 1);
            -- output 1
        @endcode

        @param expr1   Condition expression
        @param expr2   Return value if expr1 is not null
        @param expr3   Return value if expr1 is null

        @since 0.2.3
    )");
}

void DefaultUdfLibrary::InitTypeUdf() {
    RegisterExternal("double")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, double*, bool*)>(
                v1::string_to_double)))
        .return_by_arg(true)
        .returns<Nullable<double>>()
        .doc(R"(
            @brief Cast string expression to double

            Example:

            @code{.sql}
                select double("1.23");
                -- output 1.23
            @endcode
            @since 0.1.0)");
    RegisterExternal("float")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, float*, bool*)>(
                v1::string_to_float)))
        .return_by_arg(true)
        .returns<Nullable<float>>()
        .doc(R"(
            @brief Cast string expression to float

            Example:

            @code{.sql}
                select float("1.23");
                -- output 1.23
            @endcode
            @since 0.1.0)");
    RegisterExternal("int32")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, int32_t*, bool*)>(
                v1::string_to_int)))
        .return_by_arg(true)
        .returns<Nullable<int32_t>>()
        .doc(R"(
            @brief Cast string expression to int32

            Example:

            @code{.sql}
                select int32("12345");
                -- output 12345
            @endcode
            @since 0.1.0)");
    RegisterExternal("int64")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, int64_t*, bool*)>(
                v1::string_to_bigint)))
        .return_by_arg(true)
        .returns<Nullable<int64_t>>()
        .doc(R"(
            @brief Cast string expression to int64

            Example:

            @code{.sql}
                select int64("1590115420000");
                -- output 1590115420000
            @endcode
            @since 0.1.0
        )");
    RegisterExternal("int16")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, int16_t*, bool*)>(
                v1::string_to_smallint)))
        .return_by_arg(true)
        .returns<Nullable<int16_t>>()
        .doc(R"(
            @brief Cast string expression to int16

            Example:

            @code{.sql}
                select int16("123");
                -- output 123
            @endcode
            @since 0.1.0
        )");
    RegisterExternal("bool")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, bool*, bool*)>(
                v1::string_to_bool)))
        .return_by_arg(true)
        .returns<Nullable<bool>>()
        .doc(R"(
            @brief Cast string expression to bool

            Example:

            @code{.sql}
                select bool("true");
                -- output true
            @endcode
            @since 0.1.0
        )");

    RegisterExternal("date")
        .args<codec::Timestamp>(reinterpret_cast<void*>(
            static_cast<void (*)(Timestamp*, Date*, bool*)>(
                v1::timestamp_to_date)))
        .return_by_arg(true)
        .returns<Nullable<Date>>()
        .doc(R"(
            @brief Cast timestamp or string expression to date

            Example:

            @code{.sql}
                select date(timestamp(1590115420000));
                -- output 2020-05-22
                select date("2020-05-22");
                -- output 2020-05-22
            @endcode
            @since 0.1.0)");
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
        .returns<Nullable<Timestamp>>()
        .doc(R"(
            @brief Cast int64, date or string expression to timestamp

            Supported string style:
              - yyyy-mm-dd
              - yyyymmdd
              - yyyy-mm-dd hh:mm:ss

            Example:

            @code{.sql}
                select timestamp(1590115420000);
                -- output 2020-05-22 10:43:40

                select date("2020-05-22");
                -- output 2020-05-22 00:00:00

                select timestamp("2020-05-22 10:43:40");
                -- output 2020-05-22 10:43:40
            @endcode
            @since 0.1.0)");
    RegisterExternal("timestamp")
        .args<codec::StringRef>(reinterpret_cast<void*>(
            static_cast<void (*)(StringRef*, Timestamp*, bool*)>(
                v1::string_to_timestamp)))
        .return_by_arg(true)
        .returns<Nullable<Timestamp>>();
}

void DefaultUdfLibrary::InitTimeAndDateUdf() {
    RegisterExternal("year")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::year))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::year))
        .doc(R"(
            @brief Return the year part of a timestamp or date

            Example:
            @code{.sql}
                select year(timestamp(1590115420000));
                -- output 2020
            @endcode
            @since 0.1.0
        )");

    RegisterCodeGenUdf("year")
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
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::month))
        .doc(R"(
            @brief Return the month part of a timestamp or date

            Example:
            @code{.sql}
                select month(timestamp(1590115420000));
                -- output 5
            @endcode
            @since 0.1.0
        )");

    RegisterCodeGenUdf("month")
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
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::dayofmonth))
        .doc(R"(
            @brief Return the day of the month for a timestamp or date.

            Note: This function equals the `day()` function.

            Example:
            @code{.sql}
                select dayofmonth(timestamp(1590115420000));
                -- output 22

                select day(timestamp(1590115420000));
                -- output 22
            @endcode
            @since 0.1.0
        )");

    RegisterCodeGenUdf("dayofmonth").args<Date>(
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
        .args<Date>(static_cast<int32_t (*)(Date*)>(v1::dayofweek))
        .doc(R"(
            @brief Return the day of week for a timestamp or date.

            Note: This function equals the `week()` function.

            Example:
            @code{.sql}
                select dayofweek(timestamp(1590115420000));
                -- output 6
            @endcode
            @since 0.4.0
        )");

    RegisterExternal("dayofyear")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::dayofyear))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::dayofyear))
        .args<Date>(static_cast<int32_t (*)(Date*)>(v1::dayofyear))
        .doc(R"(
            @brief Return the day of year for a timestamp or date. Returns 0 given an invalid date.

            Example:
            @code{.sql}
                select dayofyear(timestamp(1590115420000));
                -- output 143

                select dayofyear(1590115420000);
                -- output 143

                select dayofyear(date("2020-05-22"));
                -- output 143

                select dayofyear(date("2020-05-32"));
                -- output 0
            @endcode
            @since 0.1.0
        )");

    RegisterExternal("weekofyear")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::weekofyear))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::weekofyear))
        .args<Date>(static_cast<int32_t (*)(Date*)>(v1::weekofyear))
        .doc(R"(
            @brief Return the week of year for a timestamp or date.

            Example:
            @code{.sql}
                select weekofyear(timestamp(1590115420000));
                -- output 21
                select week(timestamp(1590115420000));
                -- output 21
            @endcode
            @since 0.1.0
        )");

    RegisterAlias("week", "weekofyear");

    RegisterExternalTemplate<v1::IncOne>("inc")
        .args_in<int16_t, int32_t, int64_t, float, double>()
        .doc(R"(
            @brief Return expression + 1

            Example:
            @code{.sql}
                select inc(1);
                -- output 2
            @endcode
            @since 0.1.0
        )");

    RegisterCodeGenUdfTemplate<BuildGetHourUdf>("hour")
        .args_in<int64_t, Timestamp>()
        .returns<int32_t>()
        .doc(R"(
            @brief Return the hour for a timestamp

            Example:
            @code{.sql}
                select hour(timestamp(1590115420000));
                -- output 10
            @endcode
            @since 0.1.0
        )");

    RegisterCodeGenUdfTemplate<BuildGetMinuteUdf>("minute")
        .args_in<int64_t, Timestamp>()
        .returns<int32_t>()
        .doc(R"(
            @brief Return the minute for a timestamp

            Example:
            @code{.sql}
                select minute(timestamp(1590115420000));
                -- output 43
            @endcode
            @since 0.1.0
        )");

    RegisterCodeGenUdfTemplate<BuildGetSecondUdf>("second")
        .args_in<int64_t, Timestamp>()
        .returns<int32_t>()
        .doc(R"(
            @brief Return the second for a timestamp

            Example:
            @code{.sql}
                select second(timestamp(1590115420000));
                -- output 40
            @endcode
            @since 0.1.0
        )");

    RegisterExprUdf("identity")
        .doc(R"(
            @brief Return value

            Example:
            @code{.sql}
                select identity(1);
                -- output 1
            @endcode
            @since 0.1.0
        )")
        .args<AnyArg>([](UdfResolveContext* ctx, ExprNode* x) { return x; });

    RegisterExprUdf("add").args<AnyArg, AnyArg>([](UdfResolveContext* ctx,
                                                   ExprNode* x, ExprNode* y) {
        return ctx->node_manager()->MakeBinaryExprNode(x, y, node::kFnOpAdd);
        })
        .doc(R"(
            @brief Compute sum of two arguments

            Example:

            @code{.sql}
                select add(1, 2);
                -- output 3
            @endcode
            @since 0.1.0)");

    RegisterCodeGenUdf("make_tuple")
        .variadic_args<>(
            /* infer */
            [](UdfResolveContext* ctx,
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

void DefaultUdfLibrary::Init() {
    udf::RegisterNativeUdfToModule(this->node_manager());
    InitLogicalUdf();
    InitTimeAndDateUdf();
    InitTypeUdf();
    InitMathUdf();
    InitStringUdf();

    InitWindowFunctions();
    InitUdaf();
    InitFeatureZero();
}

void DefaultUdfLibrary::InitUdaf() {
    RegisterUdafTemplate<SumUdafDef>("sum")
        .doc(R"(
            @brief Compute sum of values.

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT sum(value) OVER w;
                -- output 10
            @endcode

        )")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp>();

    RegisterExprUdf("minimum").args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) {
            auto nm = ctx->node_manager();
            auto cond = nm->MakeBinaryExprNode(x, y, node::kFnOpLt);
            return nm->MakeCondExpr(cond, x, y);
        })
        .doc(R"(
            @brief Compute minimum of two arguments

            @since 0.1.0
        )");

    RegisterExprUdf("maximum").args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) {
            auto nm = ctx->node_manager();
            auto cond = nm->MakeBinaryExprNode(x, y, node::kFnOpGt);
            return nm->MakeCondExpr(cond, x, y);
        })
        .doc(R"(
            @brief Compute maximum of two arguments

            @since 0.1.0
        )");

    RegisterUdafTemplate<MinUdafDef>("min")
        .doc(R"(
            @brief Compute minimum of values.

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT min(value) OVER w;
                -- output 0
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUdafTemplate<MaxUdafDef>("max")
        .doc(R"(
            @brief Compute maximum of values.

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT max(value) OVER w;
                -- output 4
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUdafTemplate<CountUdafDef>("count")
        .doc(R"(
            @brief Compute number of values.

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT count(value) OVER w;
                -- output 5
            @endcode
            @since 0.1.0
        )")
        .args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp,
                 Date, StringRef, LiteralTypedRow<>>();


    RegisterUdafTemplate<AvgUdafDef>("avg")
        .doc(R"(
            @brief Compute average of values.

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT avg(value) OVER w;
                -- output 2
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUdafTemplate<DistinctCountDef>("distinct_count")
        .doc(R"(
            @brief Compute number of distinct values.

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |0|
            |2|
            |2|
            |4|
            @code{.sql}
                SELECT distinct_count(value) OVER w;
                -- output 3
            @endcode
            @since 0.1.0
        )")
        .args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp,
                 Date, StringRef>();

    RegisterUdafTemplate<SumWhereDef>("sum_where")
        .doc(R"(
            @brief Compute sum of values match specified condition

            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT sum_where(value, value > 2) OVER w;
                -- output 7
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUdafTemplate<CountWhereDef>("count_where")
        .doc(R"(
            @brief Compute number of values match specified condition

            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT count_where(value, value > 2) OVER w;
                -- output 2
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUdafTemplate<AvgWhereDef>("avg_where")
        .doc(R"(
            @brief Compute average of values match specified condition

            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT avg_where(value, value > 2) OVER w;
                -- output 3.5
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUdafTemplate<MinWhereDef>("min_where")
        .doc(R"(
            @brief Compute minimum of values match specified condition

            @param value  Specify value column to aggregate on.
            @param condition  Specify condition column.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT min_where(value, value > 2) OVER w;
                -- output 3
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUdafTemplate<MaxWhereDef>("max_where")
        .doc(R"(
            @brief Compute maximum of values match specified condition

            @param value Specify value column to aggregate on.
            @param condition  Specify condition column.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT max_where(value, value <= 2) OVER w;
                -- output 2
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double>();


    RegisterUdafTemplate<TopKDef>("top")
        .doc(R"(
            @brief Compute top k of values and output string separated by comma.
            The outputs are sorted in desc order

            @param value  Specify value column to aggregate on.
            @param k  Fetch top n keys.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT top(value, 3) OVER w;
                -- output "2,3,4"
            @endcode
            @since 0.1.0
        )")
        .args_in<int16_t, int32_t, int64_t, float, double, Date, Timestamp,
                 StringRef>();

    InitAggByCateUdafs();
}

}  // namespace udf
}  // namespace hybridse
