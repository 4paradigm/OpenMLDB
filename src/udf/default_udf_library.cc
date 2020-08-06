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
           const std::vector<const node::TypeNode*>& arg_types) {
            return ctx->node_manager()->MakeTypeNode(node::kVarchar);
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
            [](UDFResolveContext* ctx, const node::TypeNode* arg,
               const std::vector<const node::TypeNode*>& arg_types) {
                return ctx->node_manager()->MakeTypeNode(node::kVarchar);
            },
            /* gen */
            [](CodeGenContext* ctx, NativeValue arg,
               const std::vector<NativeValue>& args, NativeValue* out) {
                codegen::StringIRBuilder string_ir_builder(ctx->GetModule());

                return string_ir_builder.ConcatWS(ctx->GetCurrentBlock(), arg,
                                                  args, out);
            });

    RegisterExternal("substring")
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
)")
        .args<StringRef, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true);

    RegisterExternal("substring")
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
            )")
        .args<StringRef, int32_t, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true);

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
                ctx->SetError("log do not support type " +
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
        .args<int64_t>(static_cast<int64_t (*)(int64_t)>(labs));
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
        .doc("Compute sum of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date>();

    RegisterExternalTemplate<v1::Minimum>("min")
        .args_in<int16_t, int32_t, int64_t, float, double>();
    RegisterExternalTemplate<v1::StructMinimum>("min")
        .args_in<Timestamp, Date, StringRef>();

    RegisterUDAFTemplate<MinUDAFDef>("min")
        .doc("Compute min of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterExternalTemplate<v1::Maximum>("max")
        .args_in<int16_t, int32_t, int64_t, float, double>();
    RegisterExternalTemplate<v1::StructMaximum>("max")
        .args_in<Timestamp, Date, StringRef>();

    RegisterUDAFTemplate<MaxUDAFDef>("max")
        .doc("Compute max of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterUDAFTemplate<CountUDAFDef>("count")
        .doc("Compute count of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    RegisterExprUDFTemplate<AvgUDAFDef>("avg")
        .doc("Compute average of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date>();

    RegisterUDAFTemplate<DistinctCountDef>("distinct_count")
        .doc("Compute distinct number of values")
        .args_in<int16_t, int32_t, int64_t, float, double, Timestamp, Date,
                 StringRef>();

    IniMathUDF();
    InitStringUDF();
}

}  // namespace udf
}  // namespace fesql
