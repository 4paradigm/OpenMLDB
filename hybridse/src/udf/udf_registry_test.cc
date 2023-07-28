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

#include "udf/udf_registry.h"
#include <gtest/gtest.h>
#include "codegen/context.h"

namespace hybridse {
namespace udf {

using openmldb::base::Date;
using openmldb::base::Timestamp;
using hybridse::base::Status;
using hybridse::codegen::CodeGenContext;
using hybridse::codegen::NativeValue;
using hybridse::node::ExprAnalysisContext;
using hybridse::node::ExprNode;
using hybridse::node::NodeManager;
using hybridse::node::TypeNode;

class UdfRegistryTest : public ::testing::Test {
 public:
    node::NodeManager nm;
};

template <typename... LiteralArgTypes>
const node::FnDefNode* GetFnDef(UdfLibrary* lib, const std::string& name,
                                node::NodeManager* nm,
                                const node::SqlNode* over = nullptr) {
    std::vector<node::TypeNode*> arg_types(
        {DataTypeTrait<LiteralArgTypes>::to_type_node(nm)...});
    std::vector<node::ExprNode*> arg_list;
    for (size_t i = 0; i < arg_types.size(); ++i) {
        std::string arg_name = "arg_" + std::to_string(i);
        auto expr = nm->MakeExprIdNode(arg_name);
        expr->SetOutputType(arg_types[i]);
        arg_list.push_back(expr);
    }
    node::ExprNode* transformed = nullptr;
    auto status = lib->Transform(name, arg_list, nm, &transformed);
    if (!status.isOK()) {
        LOG(WARNING) << status;
        return nullptr;
    }
    if (transformed->GetExprType() != node::kExprCall) {
        LOG(WARNING) << "Resolved result is not call expr";
        return nullptr;
    }
    return reinterpret_cast<node::CallExprNode*>(transformed)->GetFnDef();
}

TEST_F(UdfRegistryTest, test_expr_udf_register) {
    UdfLibrary library;
    const node::FnDefNode* fn_def;

    // define "add"
    library.RegisterExprUdf("add")
        .args<AnyArg, AnyArg>(
            [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) {
                // extra check logic
                auto ltype = x->GetOutputType();
                auto rtype = y->GetOutputType();
                if (ltype && rtype && !ltype->Equals(rtype)) {
                    ctx->SetError("Left and right known type should equal");
                }
                return ctx->node_manager()->MakeBinaryExprNode(x, y,
                                                               node::kFnOpAdd);
            })

        .args<double, int32_t>(
            [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) {
                return ctx->node_manager()->MakeBinaryExprNode(x, y,
                                                               node::kFnOpAdd);
            })

        .args<StringRef, AnyArg, bool>(
            [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y, ExprNode* z) {
                return ctx->node_manager()->MakeBinaryExprNode(x, y,
                                                               node::kFnOpAdd);
            });

    // resolve "add"
    // match placeholder
    fn_def = GetFnDef<int32_t, int32_t>(&library, "add", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);

    // match argument num
    fn_def = GetFnDef<int32_t, int32_t, int32_t>(&library, "add", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // match but impl logic error
    fn_def = GetFnDef<int32_t, float>(&library, "add", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // match explicit
    fn_def = GetFnDef<double, int32_t>(&library, "add", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);

    // match with unknown input arg type
    fn_def = GetFnDef<int16_t, int16_t>(&library, "add", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);

    // match different argument num
    fn_def = GetFnDef<StringRef, int64_t, bool>(&library, "add", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);
}

TEST_F(UdfRegistryTest, test_variadic_expr_udf_register) {
    UdfLibrary library;
    const node::FnDefNode* fn_def;

    // define "join"
    library.RegisterExprUdf("join")
        .args<StringRef, StringRef>(
            [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) { return x; })

        .args<StringRef, AnyArg>(
            [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) {
                if (y->GetOutputType()->GetName() != "int32") {
                    ctx->SetError("Join arg type error");
                }
                return y;
            })

        .variadic_args<StringRef>([](UdfResolveContext* ctx, ExprNode* split,
                                     const std::vector<ExprNode*>& other) {
            if (other.size() < 3) {
                ctx->SetError("Join tail args error");
            }
            return split;
        })

        .variadic_args<AnyArg>([](UdfResolveContext* ctx, ExprNode* split,
                                  const std::vector<ExprNode*>& other) {
            if (other.size() < 2) {
                ctx->SetError("Join tail args error with any split");
            }
            return split;
        });

    // resolve "join"
    // illegal arg types
    fn_def = GetFnDef<int32_t, int32_t>(&library, "join", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // prefer match non-variadic, prefer explicit match
    fn_def = GetFnDef<StringRef, StringRef>(&library, "join", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);

    // prefer match non-variadic, allow placeholder
    fn_def = GetFnDef<StringRef, int32_t>(&library, "join", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);

    // prefer match non-variadic, but placeholder check failed
    fn_def = GetFnDef<StringRef, int64_t>(&library, "join", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // match variadic, prefer no-placeholder match
    fn_def = GetFnDef<StringRef, bool, bool, bool>(&library, "join", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);

    // match variadic, prefer no-placeholder match, buf impl logic failed
    fn_def = GetFnDef<StringRef, StringRef, StringRef>(&library, "join", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // match variadic with placeholder
    fn_def = GetFnDef<int16_t, StringRef, StringRef>(&library, "join", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);
}

TEST_F(UdfRegistryTest, test_variadic_expr_udf_register_order) {
    UdfLibrary library;
    const node::FnDefNode* fn_def;

    // define "concat"
    library.RegisterExprUdf("concat")
        .variadic_args<>(
            [](UdfResolveContext* ctx, const std::vector<ExprNode*> other) {
                return ctx->node_manager()->MakeExprIdNode("concat");
            })

        .variadic_args<int32_t>([](UdfResolveContext* ctx, ExprNode* x,
                                   const std::vector<ExprNode*> other) {
            if (other.size() > 2) {
                ctx->SetError("Error");
            }
            return ctx->node_manager()->MakeExprIdNode("concat");
        });

    // resolve "concat"
    // prefer long non-variadic parameters part
    fn_def = GetFnDef<int32_t, StringRef, StringRef>(&library, "concat", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);
    fn_def = GetFnDef<int32_t, bool, bool, bool>(&library, "concat", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // empty variadic args
    fn_def = GetFnDef<>(&library, "concat", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kLambdaDef);
}

TEST_F(UdfRegistryTest, test_external_udf_register) {
    UdfLibrary library;
    const node::ExternalFnDefNode* fn_def;

    // define "add"
    library.RegisterExternal("add")
        .args<AnyArg, AnyArg>("add_any", reinterpret_cast<void*>(0x01))
        .returns<int32_t>()

        .args<double, double>("add_double", reinterpret_cast<void*>(0x02))
        .returns<double>()

        .args<StringRef, AnyArg, bool>("add_string",
                                       reinterpret_cast<void*>(0x03))
        .returns<StringRef>();

    // resolve "add"
    // match placeholder
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, int32_t>(&library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("add_any", fn_def->function_name());
    ASSERT_EQ("int32", fn_def->ret_type()->GetName());

    // match argument num
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, int32_t, int32_t>(&library, "add", &nm));
    ASSERT_TRUE(fn_def == nullptr);

    // match explicit
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<double, double>(&library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("add_double", fn_def->function_name());
    ASSERT_EQ("double", fn_def->ret_type()->GetName());

    // match with unknown input arg type
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, AnyArg>(&library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("add_any", fn_def->function_name());
    ASSERT_EQ("int32", fn_def->ret_type()->GetName());

    // match different argument num
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<StringRef, int64_t, bool>(&library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("add_string", fn_def->function_name());
    ASSERT_EQ("string", fn_def->ret_type()->GetName());
}

TEST_F(UdfRegistryTest, test_variadic_external_udf_register) {
    UdfLibrary library;
    const node::ExternalFnDefNode* fn_def;

    // define "join"
    library.RegisterExternal("join")
        .args<StringRef, StringRef>("join2", nullptr)
        .returns<StringRef>()

        .args<StringRef, AnyArg>("join22", nullptr)
        .returns<StringRef>()

        .variadic_args<StringRef>("join_many", nullptr)
        .returns<StringRef>()

        .variadic_args<AnyArg>("join_many2", nullptr)
        .returns<StringRef>();

    // resolve "join"
    // prefer match non-variadic, prefer explicit match
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<StringRef, StringRef>(&library, "join", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("join2", fn_def->function_name());
    ASSERT_EQ("string", fn_def->ret_type()->GetName());

    // prefer match non-variadic, allow placeholder
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<StringRef, int32_t>(&library, "join", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("join22", fn_def->function_name());

    // match variadic, prefer no-placeholder match
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<StringRef, bool, bool, bool>(&library, "join", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("join_many", fn_def->function_name());

    // match variadic with placeholder
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int16_t, StringRef, StringRef>(&library, "join", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("join_many2", fn_def->function_name());
}

TEST_F(UdfRegistryTest, test_variadic_external_udf_register_order) {
    UdfLibrary library;
    const node::ExternalFnDefNode* fn_def;

    // define "concat"
    library.RegisterExternal("concat")
        .variadic_args<>("concat0", nullptr)
        .returns<StringRef>()
        .variadic_args<int32_t>("concat1", nullptr)
        .returns<StringRef>();

    // resolve "concat"
    // prefer long non-variadic parameters part
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, StringRef, StringRef>(&library, "concat", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("concat1", fn_def->function_name());
    ASSERT_EQ(1, fn_def->variadic_pos());

    // empty variadic args
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<>(&library, "concat", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("concat0", fn_def->function_name());
    ASSERT_EQ(0, fn_def->variadic_pos());
}

TEST_F(UdfRegistryTest, test_simple_udaf_register) {
    UdfLibrary library;

    const node::UdafDefNode* fn_def;

    library.RegisterExprUdf("add").args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* x, ExprNode* y) {
            auto res =
                ctx->node_manager()->MakeBinaryExprNode(x, y, node::kFnOpAdd);
            res->SetOutputType(x->GetOutputType());
            res->SetNullable(false);
            return res;
        });

    library.RegisterExprUdf("identity")
        .args<AnyArg>([](UdfResolveContext* ctx, ExprNode* x) { return x; });

    library.RegisterUdaf("sum")
        .templates<int32_t, int32_t, int32_t>()
        .const_init(0)
        .update("add")
        .merge("add")
        .output("identity")
        .templates<double, double, float>()
        .const_init(0.0)
        .update("add")
        .merge("add")
        .output("identity")
        .finalize();

    fn_def = dynamic_cast<const node::UdafDefNode*>(
        GetFnDef<codec::ListRef<int32_t>>(&library, "sum", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUdafDef);

    fn_def = dynamic_cast<const node::UdafDefNode*>(
        GetFnDef<codec::ListRef<int32_t>>(&library, "sum", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUdafDef);

    fn_def = dynamic_cast<const node::UdafDefNode*>(
        GetFnDef<codec::ListRef<StringRef>>(&library, "sum", &nm));
    ASSERT_TRUE(fn_def == nullptr);
}

TEST_F(UdfRegistryTest, test_codegen_udf_register) {
    UdfLibrary library;
    const node::UdfByCodeGenDefNode* fn_def;

    library.RegisterCodeGenUdf("add").args<AnyArg, AnyArg>(
        /* infer */
        [](UdfResolveContext* ctx, const ExprAttrNode* x, const ExprAttrNode* y,
           ExprAttrNode* out) {
            out->SetType(x->type());
            return Status::OK();
        },
        /* gen */
        [](CodeGenContext* ctx, NativeValue x, NativeValue y,
           NativeValue* out) {
            *out = x;
            return Status::OK();
        });

    fn_def = dynamic_cast<const node::UdfByCodeGenDefNode*>(
        GetFnDef<int32_t, int32_t>(&library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
                fn_def->GetType() == node::kUdfByCodeGenDef);
}

TEST_F(UdfRegistryTest, test_variadic_codegen_udf_register) {
    UdfLibrary library;
    const node::UdfByCodeGenDefNode* fn_def;

    library.RegisterCodeGenUdf("concat").variadic_args<>(
        /* infer */
        [](UdfResolveContext* ctx,
           const std::vector<const ExprAttrNode*>& arg_attrs,
           ExprAttrNode* out) {
            out->SetType(arg_attrs[0]->type());
            return Status::OK();
        },
        /* gen */
        [](CodeGenContext* ctx, const std::vector<NativeValue>& args,
           NativeValue* out) {
            *out = args[0];
            return Status::OK();
        });

    fn_def = dynamic_cast<const node::UdfByCodeGenDefNode*>(
        GetFnDef<int32_t, int32_t>(&library, "concat", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
                fn_def->GetType() == node::kUdfByCodeGenDef);
}

template <typename A, typename B, typename C, typename D>
void StaticSignatureCheck() {
    using Check = FuncTypeCheckHelper<A, B, C, D>;
    static_assert(Check::value, "error");
}

template <typename A, typename B, typename C, typename D>
void StaticSignatureCheckFail() {
    using Check = FuncTypeCheckHelper<A, B, C, D>;
    static_assert(!Check::value, "error");
}

TEST_F(UdfRegistryTest, StaticExternSignatureCheck) {
    using openmldb::base::StringRef;

    // normal arg
    StaticSignatureCheck<int, std::tuple<int, int>, int,
                         std::tuple<int, int>>();

    // c arg not enough
    StaticSignatureCheckFail<int, std::tuple<int, int>, int, std::tuple<int>>();

    // c arg is too much
    StaticSignatureCheckFail<int, std::tuple<int>, int, std::tuple<int, int>>();

    // return by arg
    StaticSignatureCheck<int, std::tuple<int>, void, std::tuple<int, int*>>();
    StaticSignatureCheckFail<int, std::tuple<int>, void, std::tuple<int>>();

    // struct arg
    StaticSignatureCheck<int, std::tuple<int, StringRef>, int,
                         std::tuple<int, StringRef*>>();
    StaticSignatureCheckFail<int, std::tuple<int, StringRef>, int,
                             std::tuple<int, StringRef>>();

    // struct return
    StaticSignatureCheck<Date, std::tuple<Date, Date>, void,
                         std::tuple<Date*, Date*, Date*>>();
    StaticSignatureCheckFail<Date, std::tuple<Date, Date>, void,
                             std::tuple<Date*, Date*, Date>>();

    // nullable arg
    StaticSignatureCheck<int, std::tuple<int, Nullable<int>, int>, int,
                         std::tuple<int, int, bool, int>>();
    StaticSignatureCheckFail<int, std::tuple<int, Nullable<int>, int>, int,
                             std::tuple<int, int, int>>();
    StaticSignatureCheck<int, std::tuple<int, Nullable<StringRef>, int>, int,
                         std::tuple<int, StringRef*, bool, int>>();
    StaticSignatureCheckFail<int, std::tuple<int, Nullable<StringRef>, int>,
                             int, std::tuple<int, StringRef, bool, int>>();

    // nullable return
    StaticSignatureCheck<Nullable<int>, std::tuple<>, void,
                         std::tuple<int*, bool*>>();
    StaticSignatureCheckFail<Nullable<int>, std::tuple<>, void,
                             std::tuple<int*>>();

    // nullable arg and return
    StaticSignatureCheck<Nullable<Date>, std::tuple<Nullable<int>>, void,
                         std::tuple<int, bool, Date*, bool*>>();

    // tuple arg
    StaticSignatureCheck<int, std::tuple<Tuple<Nullable<int>, int>>, int,
                         std::tuple<int, bool, int>>();
    StaticSignatureCheckFail<int, std::tuple<Tuple<>>, int, std::tuple<>>();

    // nested tuple arg
    StaticSignatureCheck<
        int, std::tuple<Tuple<float, Tuple<float, Nullable<int>>, int>>, int,
        std::tuple<float, float, int, bool, int>>();

    // tuple return
    StaticSignatureCheck<Tuple<double, Nullable<int>, Nullable<StringRef>>,
                         std::tuple<>, void,
                         std::tuple<double*, int*, bool*, StringRef*, bool*>>();

    // nest tuple return
    StaticSignatureCheck<
        Tuple<Tuple<Date, int>, Tuple<Nullable<float>>>,
        std::tuple<Tuple<int, Nullable<Date>>>, void,
        std::tuple<int, Date*, bool, Date*, int*, float*, bool*>>();

    // opaque
    StaticSignatureCheck<Opaque<std::string>,
                         std::tuple<Opaque<std::string>, int>, std::string*,
                         std::tuple<std::string*, int>>();
}

}  // namespace udf
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
