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
#include "codegen/date_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"

using fesql::codec::Date;
using fesql::codec::StringRef;
using fesql::codec::Timestamp;
using fesql::codegen::CodeGenContext;
using fesql::codegen::NativeValue;
using fesql::node::TypeNode;

namespace fesql {
namespace udf {

template <typename T>
struct BuildGetHourUDF {
    using LiteralArgTypes = std::tuple<T>;

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
    using LiteralArgTypes = std::tuple<T>;

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
    using LiteralArgTypes = std::tuple<T>;

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
    void operator()(SimpleUDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T>()
            .const_init(T(0))
            .update("add")
            .merge("add")
            .output("identity");
    }
};

template <typename T>
struct MinUDAFDef {
    void operator()(SimpleUDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T>().update("min").merge("min").output(
            "identity");
    }
};

template <typename T>
struct MaxUDAFDef {
    void operator()(SimpleUDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, T, T>().update("max").merge("max").output(
            "identity");
    }
};

template <typename T>
struct CountUDAFDef {
    void operator()(SimpleUDAFRegistryHelper& helper) {  // NOLINT
        helper.templates<T, int64_t, int64_t>()
            .const_init(0)
            .update([](UDFResolveContext* ctx, const TypeNode* s,
                       const TypeNode*) { return s; },
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
    using LiteralArgTypes = std::tuple<codec::ListRef<T>>;

    ExprNode* operator()(UDFResolveContext* ctx, ExprNode* input) {
        auto nm = ctx->node_manager();
        auto sum = nm->MakeFuncNode("sum", {input}, ctx->over());
        auto cnt = nm->MakeFuncNode("count", {input}, ctx->over());
        sum = nm->MakeCastNode(node::kDouble, sum);
        auto avg = nm->MakeBinaryExprNode(sum, cnt, node::kFnOpFDiv);
        avg->SetOutputType(nm->MakeTypeNode(node::kDouble));
        return avg;
    }
};

template <typename T>
struct DistinctCountDef {
    void operator()(SimpleUDAFRegistryHelper& helper) {  // NOLINT
        std::string suffix = ".opaque_std_set_" + DataTypeTrait<T>::to_string();
        helper.templates<T, Opaque<std::unordered_set<T>>, int64_t>()
            .init("distinct_count_init" + suffix, init_set)
            .update("distinct_count_update" + suffix, update_set)
            .output("distinct_count_output" + suffix, set_size);
    }

    static void init_set(std::unordered_set<T>* addr) {
        new (addr) std::unordered_set<T>();
    }

    static std::unordered_set<T>* update_set(std::unordered_set<T>* set,
                                             T value) {
        set->insert(value);
        return set;
    }

    static int64_t set_size(std::unordered_set<T>* set) {
        int64_t size = set->size();
        set->clear();
        using SetT = std::unordered_set<T>;
        set->~SetT();
        return size;
    }
};

void DefaultUDFLibrary::InitStringUDF() {
    RegisterCodeGenUDF("concat").variadic_args<>(
        /* infer */ [](UDFResolveContext* ctx,
                       const std::vector<const node::TypeNode*>&
                           arg_types) {
           return ctx->node_manager()->MakeTypeNode(node::kVarchar);
        },
        /* gen */
        [](CodeGenContext* ctx, const std::vector<NativeValue>& args,
           NativeValue* out) {
            codegen::StringIRBuilder string_ir_builder(ctx->GetModule());
            return string_ir_builder.Concat(ctx->GetCurrentBlock(), args, out);
        });

    RegisterExternal("substring")
        .args<StringRef, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true);

    RegisterExternal("substring")
        .args<StringRef, int32_t, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true);
    //    RegisterExprUDF("substring")
    //        .args<StringRef, AnyArg, AnyArg>([](UDFResolveContext* ctx,
    //                                             ExprNode* str, ExprNode* pos,
    //                                             ExprNode* len) -> ExprNode* {
    //            if (!pos->GetOutputType()->IsInteger()) {
    //                ctx->SetError("substring do not support pos type " +
    //                              pos->GetOutputType()->GetName());
    //                return nullptr;
    //            }
    //            if (!len->GetOutputType()->IsInteger()) {
    //                ctx->SetError("substring do not support len type " +
    //                              pos->GetOutputType()->GetName());
    //                return nullptr;
    //            }
    //            auto nm = ctx->node_manager();
    //            return nm->MakeFuncNode("substring",
    //                                    {str, nm->MakeCastNode(node::kInt32,
    //                                    pos),
    //                                     nm->MakeCastNode(node::kInt32, len)},
    //                                    nullptr);
    //        });
}
void DefaultUDFLibrary::IniMathUDF() {
    RegisterExternal("log")
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
    RegisterAlias("ln", "log");

    RegisterExternal("log2")
        .args<float>(static_cast<float (*)(float)>(log2))
        .args<double>(static_cast<double (*)(double)>(log2));
    RegisterExprUDF("log2").args<AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x) -> ExprNode* {
            if (!x->GetOutputType()->IsArithmetic()) {
                ctx->SetError("log do not support type " +
                              x->GetOutputType()->GetName());
                return nullptr;
            }
            auto nm = ctx->node_manager();
            auto cast = nm->MakeCastNode(node::kDouble, x);
            return nm->MakeFuncNode("log2", {cast}, nullptr);
        });

    RegisterExternal("log10")
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
}

void DefaultUDFLibrary::Init() {
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

    //

    RegisterAlias("lead", "at");

    RegisterCodeGenUDF("identity")
        .args<AnyArg>(
            [](UDFResolveContext* ctx, const node::TypeNode* in) { return in; },
            [](CodeGenContext* ctx, NativeValue in, NativeValue* out) {
                *out = in;
                return Status::OK();
            });

    RegisterExprUDF("add").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
            auto res =
                ctx->node_manager()->MakeBinaryExprNode(x, y, node::kFnOpAdd);
            res->SetOutputType(x->GetOutputType());
            return res;
        });

    RegisterUDAFTemplate<SumUDAFDef>("sum")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date>();

    RegisterExternalTemplate<v1::Minimum>("min")
        .args_in<int16_t, int32_t, int64_t, float, double>();
    RegisterExternalTemplate<v1::StructMinimum>("min")
        .args_in<Timestamp, Date, StringRef>();

    RegisterUDAFTemplate<MinUDAFDef>("min")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterExternalTemplate<v1::Maximum>("max")
        .args_in<int16_t, int32_t, int64_t, float, double>();
    RegisterExternalTemplate<v1::StructMaximum>("max")
        .args_in<Timestamp, Date, StringRef>();

    RegisterUDAFTemplate<MaxUDAFDef>("max")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUDAFTemplate<CountUDAFDef>("count")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterExprUDFTemplate<AvgUDAFDef>("avg")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date>();

    RegisterUDAFTemplate<DistinctCountDef>("distinct_count")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    IniMathUDF();
    InitStringUDF();
}

}  // namespace udf
}  // namespace fesql
