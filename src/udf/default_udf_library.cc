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
#include <vector>
#include "codegen/date_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"
#include "udf/containers.h"

using fesql::codec::Date;
using fesql::codec::StringRef;
using fesql::codec::Timestamp;
using fesql::codegen::CodeGenContext;
using fesql::codegen::NativeValue;
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
                   "Fail to build udf hour(int64): ", status.msg);
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
                   "Fail to build udf hour(int64): ", status.msg);
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
                   "Fail to build udf hour(int64): ", status.msg);
        *out = NativeValue::Create(ret);
        return status;
    }
};

template <typename T>
struct SumUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T>()
            .const_init(T(0))
            .update("add")
            .merge("add")
            .output("identity");
    }
};

template <typename T>
struct MinUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T>()
            .const_init(DataTypeTrait<T>::maximum_value())
            .update("minimum")
            .merge("minimum")
            .output("identity");
    }
};

template <typename T>
struct MaxUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T>()
            .const_init(DataTypeTrait<T>::minimum_value())
            .update("maximum")
            .merge("maximum")
            .output("identity");
    }
};

template <typename T>
struct CountUDAFDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<int64_t, int64_t, T>()
            .const_init(0)
            .update(
                [](UDFResolveContext* ctx, const ExprAttrNode* st,
                   const ExprAttrNode* in, ExprAttrNode* out) {
                    out->SetType(st->type());
                    out->SetNullable(false);
                    return Status::OK();
                },
                [](CodeGenContext* ctx, NativeValue cnt, NativeValue elem,
                   NativeValue* out) {
                    auto builder = ctx->GetBuilder();
                    *out = NativeValue::Create(builder->CreateAdd(
                        cnt.GetValue(builder), builder->getInt64(1)));
                    return Status::OK();
                })
            .merge("add")
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
                    auto cnt_ty = state->GetOutputType()->GetGenericType(0);
                    auto sum_ty = state->GetOutputType()->GetGenericType(1);
                    cnt = nm->MakeBinaryExprNode(cnt, nm->MakeConstNode(1),
                                                 node::kFnOpAdd);
                    sum = nm->MakeBinaryExprNode(sum, input, node::kFnOpAdd);
                    cnt->SetOutputType(cnt_ty);
                    sum->SetOutputType(sum_ty);
                    return nm->MakeFuncNode("make_tuple", {cnt, sum}, nullptr);
                })
            .output([](UDFResolveContext* ctx, ExprNode* state) {
                auto nm = ctx->node_manager();
                ExprNode* cnt = nm->MakeGetFieldExpr(state, 0);
                ExprNode* sum = nm->MakeGetFieldExpr(state, 1);
                ExprNode* avg =
                    nm->MakeBinaryExprNode(sum, cnt, node::kFnOpFDiv);
                avg->SetOutputType(state->GetOutputType()->GetGenericType(1));
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
struct CountWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<int64_t, int64_t, T, bool>()
            .const_init(0)
            .update([](UDFResolveContext* ctx, ExprNode* cnt, ExprNode* elem, ExprNode* cond) {
                auto nm = ctx->node_manager();
                auto new_cnt = nm->MakeBinaryExprNode(cnt, nm->MakeConstNode(1), node::kFnOpAdd);
                ExprNode* update = nm->MakeCondExpr(cond, new_cnt, cnt);
                return update;
            })
            .merge("add")
            .output("identity");
    }
};

template <typename T>
struct AvgWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<int64_t, Tuple<int64_t, double>, T, bool>()
            .const_init(MakeTuple((int64_t)0, 0.0))
            .update([](UDFResolveContext* ctx, ExprNode* state, ExprNode* elem, ExprNode* cond) {
                auto nm = ctx->node_manager();
                auto cnt = nm->MakeGetFieldExpr(state, 0);
                auto sum = nm->MakeGetFieldExpr(state, 1);
                auto cnt_ty = state->GetOutputType()->GetGenericType(0);
                auto sum_ty = state->GetOutputType()->GetGenericType(1);
                auto new_cnt = nm->MakeBinaryExprNode(cnt, nm->MakeConstNode(1),
                                             node::kFnOpAdd);
                if (elem->GetOutputType()->base() == node::kTimestamp) {
                    elem = nm->MakeCastNode(node::kInt64, elem);
                }
                auto new_sum = nm->MakeBinaryExprNode(sum, elem, node::kFnOpAdd);
                new_cnt->SetOutputType(cnt_ty);
                new_sum->SetOutputType(sum_ty);
                auto new_state = nm->MakeFuncNode("make_tuple", {new_cnt, new_sum}, nullptr);
                return nm->MakeCondExpr(cond, new_state, state);
            })
            .merge("add")
            .output([](UDFResolveContext* ctx, ExprNode* state) {
                auto nm = ctx->node_manager();
                ExprNode* cnt = nm->MakeGetFieldExpr(state, 0);
                ExprNode* sum = nm->MakeGetFieldExpr(state, 1);
                ExprNode* avg =
                    nm->MakeBinaryExprNode(sum, cnt, node::kFnOpFDiv);
                avg->SetOutputType(state->GetOutputType()->GetGenericType(1));
                return avg;
            });
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
        std::string suffix = ".opaque_" + DataTypeTrait<BoundT>::to_string() + "_bound_" + DataTypeTrait<T>::to_string();
        helper.templates<StringRef, Opaque<ContainerT>, Nullable<T>, BoundT>()
            .init("topk_init" + suffix, ContainerT::Init)
            .update("topk_update" + suffix, ContainerT::Push)
            .output("topk_output" + suffix, ContainerT::Output);
    }
};

template <typename K>
struct AvgCateDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()->RegisterUDAFTemplate<Impl>("avg_cate")
            .doc(helper.registry()->doc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, std::pair<int64_t, double>>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix = ".opaque_dict_" + DataTypeTrait<K>::to_string() + "_" + DataTypeTrait<V>::to_string();
            helper.templates<StringRef, Opaque<ContainerT>, Nullable<K>, Nullable<V>>()
                .init("avg_cate_init" + suffix, ContainerT::Init)
                .update("avg_cate_update" + suffix, Update)
                .output("avg_cate_output" + suffix, Output);
        }

        static ContainerT* Update(ContainerT* ptr, InputK key, bool is_key_null,
                                  InputV value, bool is_value_null) {
            if (is_key_null || is_value_null) {
                return ptr;
            }
            auto& map = ptr->map();
            auto stored_key = ContainerT::to_stored_key(key);
            auto iter = map.find(stored_key);
            if (iter == map.end()) {
                map.insert(iter, {stored_key, std::make_pair<int64_t, double>(1, ContainerT::to_stored_value(value))});
            } else {
                auto& pair = iter->second;
                pair.first += 1;
                pair.second += ContainerT::to_stored_value(value);
            }
            return ptr;
        }

        static void Output(ContainerT* ptr, codec::StringRef* output) {
            ContainerT::OutputString(ptr, false, output,
                [](const std::pair<int64_t, double>& value, char* buf, size_t size){
                    double avg = value.second / value.first;
                    return v1::format_string(avg, buf, size);
                });
            ContainerT::Destroy(ptr);
        }
    };
};

template <typename K>
struct AvgCateWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()->RegisterUDAFTemplate<Impl>("avg_cate_where")
            .doc(helper.registry()->doc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, std::pair<int64_t, double>>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        using AvgCateImpl = typename AvgCateDef<K>::template Impl<V>;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix = ".opaque_dict_" + DataTypeTrait<K>::to_string() + "_" + DataTypeTrait<V>::to_string();
            helper.templates<StringRef, Opaque<ContainerT>, Nullable<K>, Nullable<V>, Nullable<bool>>()
                .init("avg_cate_where_init" + suffix, ContainerT::Init)
                .update("avg_cate_where_update" + suffix, Update)
                .output("avg_cate_where_output" + suffix, AvgCateImpl::Output);
        }

        static ContainerT* Update(ContainerT* ptr, InputK key, bool is_key_null,
                                  InputV value, bool is_value_null,
                                  bool cond, bool is_cond_null) {
            if (cond && !is_cond_null) {
                AvgCateImpl::Update(ptr, key, is_key_null, value, is_value_null);
            }
            return ptr;
        }
    };
};

template <typename K>
struct TopAvgCateWhereDef {
    void operator()(UDAFRegistryHelper& helper) {  // NOLINT
        helper.library()->RegisterUDAFTemplate<Impl>("top_n_avg_cate_where")
            .doc(helper.registry()->doc())
            .template args_in<int16_t, int32_t, int64_t, float, double>();
    }

    template <typename V>
    struct Impl {
        using ContainerT = udf::container::BoundedGroupByDict<K, V, std::pair<int64_t, double>>;
        using InputK = typename ContainerT::InputK;
        using InputV = typename ContainerT::InputV;

        using AvgCateImpl = typename AvgCateDef<K>::template Impl<V>;

        void operator()(UDAFRegistryHelper& helper) {  // NOLINT
            std::string suffix;

            suffix = ".i32_bound_opaque_dict_" + DataTypeTrait<K>::to_string() + "_" + DataTypeTrait<V>::to_string();
            helper.templates<StringRef, Opaque<ContainerT>, Nullable<K>, Nullable<V>, Nullable<bool>, int32_t>()
                .init("top_n_avg_cate_where_init" + suffix, ContainerT::Init)
                .update("top_n_avg_cate_where_update" + suffix, UpdateI32Bound)
                .output("top_n_avg_cate_where_output" + suffix, Output);

            suffix = ".i64_bound_opaque_dict_" + DataTypeTrait<K>::to_string() + "_" + DataTypeTrait<V>::to_string();
            helper.templates<StringRef, Opaque<ContainerT>, Nullable<K>, Nullable<V>, Nullable<bool>, int64_t>()
                .init("top_n_avg_cate_where_init" + suffix, ContainerT::Init)
                .update("top_n_avg_cate_where_update" + suffix, Update)
                .output("top_n_avg_cate_where_output" + suffix, Output);
        }

        static ContainerT* Update(ContainerT* ptr, InputK key, bool is_key_null,
                                  InputV value, bool is_value_null,
                                  bool cond, bool is_cond_null, int64_t bound) {
            if (cond && !is_cond_null) {
                AvgCateImpl::Update(ptr, key, is_key_null, value, is_value_null);
                auto& map = ptr->map();
                if (map.size() > bound) {
                    map.erase(map.begin());
                }
            }
            return ptr;
        }

        static ContainerT* UpdateI32Bound(ContainerT* ptr, InputK key, bool is_key_null,
                                  InputV value, bool is_value_null,
                                  bool cond, bool is_cond_null,
                                  int32_t bound) {
            return Update(ptr, key, is_key_null, value, is_value_null, cond, is_cond_null, bound);
        }

        static void Output(ContainerT* ptr, codec::StringRef* output) {
            ContainerT::OutputString(ptr, true, output,
                [](const std::pair<int64_t, double>& value, char* buf, size_t size){
                    double avg = value.second / value.first;
                    return v1::format_string(avg, buf, size);
                });
            ContainerT::Destroy(ptr);
        }
    };
};

void DefaultUDFLibrary::InitStringUDF() {
    RegisterExternalTemplate<v1::ToString>("string")
        .args_in<int16_t, int32_t, int64_t, float, double>()
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

            @param **str**
            @param **pos** define the begining of the substring.

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

            example
            @code{.sql}

                select substr("hello world", 3, 6);
                -- output "llo wo"

            @endcode

            @param **str**
            @param **pos**: define the begining of the substring.

             - If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
             - If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

            @param **len** length of substring. If len is less than 1, the result is the empty string.

            @since 2.0.0.0
        )");

    RegisterAlias("substr", "substring");

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

example
@code{.sql}

    SELECT LOG(1);  
    -- output 0.000000

    SELECT LOG(10,100);
    -- output 2
@endcode

@param **base**
@param **expr**

@since 2.0.0.0
)")
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
    RegisterExprUDF("log")
        .args<AnyArg, AnyArg>(
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

example
@code{.sql}

    SELECT LN(1);  
    -- output 0.000000

@endcode

@param **expr**

@since 2.0.0.0
)")
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

example
@code{.sql}

    SELECT LOG2(65536);  
    -- output 16

@endcode

@param **expr**

@since 2.0.0.0
)")
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

example
@code{.sql}

    SELECT LOG10(100);  
    -- output 2

@endcode

@param **expr**

@since 2.0.0.0
)")
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

    RegisterExternal("abs")
.doc(R"(
Return the absolute value of expr.

example
@code{.sql}

    SELECT ABS(-32);
    -- output 32

@endcode

@param **expr**

@since 2.0.0.0
)")
        .args<int16_t>(static_cast<int16_t (*)(int16_t)>(v1::abs_int16))
        .args<int32_t>(static_cast<int32_t (*)(int32_t)>(abs));
    RegisterExternal("abs")
        .args<int64_t>(static_cast<int64_t (*)(int64_t)>(v1::abs_int64));
    RegisterExternal("abs")
        .args<float>(static_cast<float (*)(float)>(fabs))
        .args<double>(static_cast<double (*)(double)>(fabs));

    RegisterExternalTemplate<v1::Acos>("acos")
.doc(R"(
Return the arc cosine of expr.

example
@code{.sql}

    SELECT ACOS(1);
    -- output 0

@endcode

@param **expr**

@since 2.0.0.0
)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("acos")
        .args<float>(static_cast<float (*)(float)>(acosf));

    RegisterExternalTemplate<v1::Asin>("asin")
.doc(R"(
Return the arc sine of expr.

example
@code{.sql}

    SELECT ASIN(0.0);
    -- output 0.000000

@endcode

@param **expr**

@since 2.0.0.0
)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("asin")
        .args<float>(static_cast<float (*)(float)>(asinf));

    RegisterExternalTemplate<v1::Atan>("atan")
.doc(R"(
atan(Y, X)
If called with one parameter, this function returns the arc tangent of expr.
If called with two parameters X and Y, this function returns the arc tangent of Y / X.

example
@code{.sql}

    SELECT ATAN(-0.0);  
    -- output -0.000000

    SELECT ATAN(0, -0);
    -- output 3.141593

@endcode

@param **X**
@param **Y**

@since 2.0.0.0
)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan")
        .args<float>(static_cast<float (*)(float)>(atanf));

    RegisterExternalTemplate<v1::Atan2>("atan")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan")
        .args<float, float>(static_cast<float (*)(float, float)>(atan2f));
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

example
@code{.sql}

    SELECT ATAN2(0, -0);
    -- output 3.141593

@endcode

@param **X**
@param **Y**

@since 2.0.0.0
)")
        .args_in<int16_t, int32_t, int64_t, double>();
    RegisterExternal("atan2")
        .args<float, float>(static_cast<float (*)(float, float)>(atan2f));
    RegisterExprUDF("atan2").args<AnyArg, AnyArg>(
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
            return nm->MakeFuncNode("atan2", {cast1, cast2}, nullptr);
        });

    RegisterExternalTemplate<v1::Ceil>("ceil")
.doc(R"(
Return the smallest integer value not less than the expr

example
@code{.sql}

    SELECT CEIL(1.23);
    -- output 2

@endcode

@param **expr**

@since 2.0.0.0
)")
        .args_in<int16_t, int32_t, int64_t>();
    RegisterExternal("ceil")
        .args<double>(static_cast<int (*)(double)>(v1::Ceild));
    RegisterExternal("ceil")
        .args<float>(static_cast<int (*)(float)>(v1::Ceilf));
    RegisterAlias("ceil", "ceiling");
}

void DefaultUDFLibrary::Init() {
    udf::RegisterNativeUDFToModule();
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
                           "Fail to build udf year(date): ", status.msg);
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
                           "Fail to build udf month(date): ", status.msg);
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
                           "Fail to build udf day(date): ", status.msg);
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

    RegisterExternalTemplate<v1::AtList>("at")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterExternalTemplate<v1::AtStructList>("at")
        .return_by_arg(true)
        .args_in<Timestamp, Date, StringRef>();

    RegisterAlias("lead", "at");

    RegisterCodeGenUDF("identity")
        .args<AnyArg>(
            [](UDFResolveContext* ctx, const ExprAttrNode* in,
               ExprAttrNode* out) {
                out->SetType(in->type());
                out->SetNullable(in->nullable());
                return Status::OK();
            },
            [](CodeGenContext* ctx, NativeValue in, NativeValue* out) {
                *out = in;
                return Status::OK();
            });

    RegisterExprUDF("add").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
            auto res =
                ctx->node_manager()->MakeBinaryExprNode(x, y, node::kFnOpAdd);
            res->SetOutputType(x->GetOutputType());
            res->SetNullable(false);
            return res;
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

    RegisterUDAFTemplate<SumUDAFDef>("sum")
        .doc("Compute sum of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date>();

    RegisterExternalTemplate<v1::Minimum>("minimum")
        .args_in<int16_t, int32_t, int64_t, float, double>();
    RegisterExternalTemplate<v1::StructMinimum>("minimum")
        .return_by_arg(true)
        .args_in<Timestamp, Date, StringRef>();

    RegisterUDAFTemplate<MinUDAFDef>("min")
        .doc("Compute min of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date>();

    RegisterExternalTemplate<v1::Maximum>("maximum")
        .args_in<int16_t, int32_t, int64_t, float, double>();
    RegisterExternalTemplate<v1::StructMaximum>("maximum")
        .return_by_arg(true)
        .args_in<Timestamp, Date, StringRef>();

    RegisterUDAFTemplate<MaxUDAFDef>("max")
        .doc("Compute max of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUDAFTemplate<CountUDAFDef>("count")
        .doc("Compute count of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef, LiteralTypedRow<>>();

    RegisterUDAFTemplate<AvgUDAFDef>("avg")
        .doc("Compute average of values")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUDAFTemplate<DistinctCountDef>("distinct_count")
        .doc("Compute distinct number of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUDAFTemplate<CountWhereDef>("count_where")
        .doc("Compute number of values match specified condition")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUDAFTemplate<AvgWhereDef>("avg_where")
        .doc("Compute average of values match specified condition")
        .args_in<int16_t, int32_t, int64_t, float, double>();

    RegisterUDAFTemplate<TopKDef>("top")
        .doc("Compute top k of values and output string separated by comma. "
             "The outputs are sorted in desc order")
        .args_in<int16_t, int32_t, int64_t, float, double, Date, Timestamp, StringRef>();

    RegisterUDAFTemplate<AvgCateDef>("avg_cate")
        .doc(R"(
            Compute average of values grouped by category key and output string.
            Each group is represented as 'K:V' and separated by comma in outputs
            and are sorted by key in ascend order.

            @param **catagory**  Specify catagory column to group by. 
            @param **value**  Specify value column to aggregate on.

            Example:
            catagory|value
            --|--
            x|0
            y|1
            x|2
            y|3
            x|4
            @code{.sql}
                # cat
                SELECT avg_cate(catagory, value) OVER w;
                -- output "x:2,y:2"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, float, double, Date, Timestamp, StringRef>();

    RegisterUDAFTemplate<AvgCateWhereDef>("avg_cate_where")
        .doc(R"(
            Compute average of values matching specified condition grouped by category key
            and output string. Each group is represented as 'K:V' and separated by comma in 
            outputs and are sorted by key in ascend order.

            @param **catagory**  Specify catagory column to group by. 
            @param **value**  Specify value column to aggregate on.
            @param **condition**  Specify condition column.

            Example:
            catagory|value|condition
            --|--|--
            x|0|true
            y|1|false
            x|2|false
            y|3|true
            x|4|true
            @code{.sql}
                # cat
                SELECT avg_cate_where(catagory, value, condition) OVER w;
                -- output "x:2,y:3"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, float, double, Date, Timestamp, StringRef>();

    RegisterUDAFTemplate<TopAvgCateWhereDef>("top_n_avg_cate_where")
        .doc(R"(
            Compute average of values matching specified condition grouped by category key.
            Output string for top N keys in descend order. Each group is represented as 'K:V'
            and separated by comma.

            @param **catagory**  Specify catagory column to group by. 
            @param **value**  Specify value column to aggregate on.
            @param **condition**  Specify condition column.
            @param **n**  Fetch top n keys.

            Example:
            catagory|value|condition
            --|--|--
            x|0|true
            y|1|false
            x|2|false
            y|3|true
            x|4|true
            z|5|true
            z|6|false
            @code{.sql}
                # cat
                SELECT avg_cate_where(catagory, value, condition, 2) OVER w;
                -- output "z:5,y:3"
            @endcode
            )")
        .args_in<int16_t, int32_t, int64_t, float, double, Date, Timestamp, StringRef>();

    IniMathUDF();
    InitStringUDF();
}

}  // namespace udf
}  // namespace fesql
