/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * default_udf_library.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/default_udf_library.h"

#include <tuple>

#include "codegen/date_ir_builder.h"
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
struct CountUDAFDef {                                    // NOLINT
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

/* template <typename T>
struct AvgUDAFDef {
    void operator()(SimpleUDAFRegistryHelper& helper) {
        helper.templates<T, TupleArg<T, int64_t>, T>
            .update([](CodeGenContext* ctx,
                       const std::tuple<NativeValue, NativeValue>& state,
                       NativeValue elem,
                       std::tuple<NativeValue, NativeValue>* out) {
                auto builder = ctx->GetBuilder();
                *out = NativeValue::Create(builder->CreateAdd(
                    cnt.GetValue(builder), builder->getInt64(1)));
                return Status::OK();
            })
    }
};*/

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

    RegisterExternal("dayofweek")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::dayofweek))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::dayofweek))
        .args<Date>(static_cast<int32_t (*)(Date*)>(v1::dayofweek));

    RegisterExternal("weekofyear")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::weekofyear))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::weekofyear))
        .args<Date>(static_cast<int32_t (*)(Date*)>(v1::weekofyear));

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
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date>();
}

}  // namespace udf
}  // namespace fesql
