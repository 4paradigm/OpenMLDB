/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_registry_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include <gtest/gtest.h>
#include "udf/udf_registry.h"

namespace fesql {
namespace udf {

using fesql::node::NodeManager;
using fesql::node::ExprNode;

class UDFRegistryTest : public ::testing::Test {};


template <typename ...LiteralArgTypes>
const node::FnDefNode* GetFnDef(const UDFLibrary& lib,
                                const std::string& name,
                                node::NodeManager* nm,
                                const node::SQLNode* over = nullptr) {
    std::vector<node::TypeNode*> arg_types(
        {ArgTypeTrait<LiteralArgTypes>::to_type_node(nm)...});
    auto arg_list = reinterpret_cast<node::ExprListNode*>(nm->MakeExprList());
    for (size_t i = 0; i < arg_types.size(); ++i) {
        std::string arg_name = "arg_" + std::to_string(i);
        auto expr = nm->MakeExprIdNode(arg_name);
        expr->SetOutputType(arg_types[i]);
        arg_list->AddChild(expr);
    }
    node::ExprNode* transformed = nullptr;
    auto status = lib.Transform(name, arg_list, over, nm, &transformed);
    if (!status.isOK()) {
        LOG(WARNING) << status.msg;
        return nullptr;
    }
    if (transformed->GetExprType() != node::kExprCall) {
        LOG(WARNING) << "Resolved result is not call expr";
        return nullptr;
    }
    return reinterpret_cast<node::CallExprNode*>(transformed)->GetFnDef();
}


TEST_F(UDFRegistryTest, test_expr_udf_register) {
    UDFLibrary library;
    node::NodeManager nm;
    auto over = nm.MakeWindowDefNode("w");
    const node::FnDefNode* fn_def;
    
    // define "add"
    library.RegisterExprUDF("add")
    	.allow_window(false)
        .allow_project(true)

    	.args<AnyArg, AnyArg>(
            [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
                // extra check logic
                auto ltype = x->GetOutputType();
                auto rtype = y->GetOutputType();
                if (ltype && rtype && !ltype->Equals(rtype)) {
                    ctx->SetError("Left and right known type should equal");
                }
                return ctx->node_manager()->MakeBinaryExprNode(
                    x, y, node::kFnOpAdd);
            })

        .args<double, int32_t>(
            [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
                return ctx->node_manager()->MakeBinaryExprNode(
                    x, y, node::kFnOpAdd);
            })

        .args<std::string, AnyArg, bool>(
            [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y, ExprNode* z) {
                return ctx->node_manager()->MakeBinaryExprNode(
                    x, y, node::kFnOpAdd);
            });

    // resolve "add"
    // match placeholder
    fn_def = GetFnDef<int32_t, int32_t>(library, "add", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);

    // allow_window(false)
    fn_def = GetFnDef<int32_t, int32_t>(library, "add", &nm, over);
    ASSERT_TRUE(fn_def == nullptr);

    // match argument num
    fn_def = GetFnDef<int32_t, int32_t, int32_t>(library, "add", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // match but impl logic error
    fn_def = GetFnDef<int32_t, float>(library, "add", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // match explicit
    fn_def = GetFnDef<double, int32_t>(library, "add", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);

    // match with unknown input arg type
    fn_def = GetFnDef<int32_t, AnyArg>(library, "add", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);

    // match different argument num
    fn_def = GetFnDef<std::string, int64_t, bool>(library, "add", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);
}


TEST_F(UDFRegistryTest, test_variadic_expr_udf_register) {
    UDFLibrary library;
    node::NodeManager nm;
    const node::FnDefNode* fn_def;
    
    // define "join"
    library.RegisterExprUDF("join")
        .args<std::string, std::string>(
            [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
                return x;
            })

        .args<std::string, AnyArg>(
            [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
                if (y->GetOutputType()->GetName() != "int32") {
                    ctx->SetError("Join arg type error");
                }
                return y;
            })

        .variadic_args<std::string>(
            [](UDFResolveContext* ctx, ExprNode* split,
                const std::vector<ExprNode*>& other) {
                if (other.size() < 3) {
                    ctx->SetError("Join tail args error");
                }
                return split;
            })

        .variadic_args<AnyArg>(
            [](UDFResolveContext* ctx, ExprNode* split,
                const std::vector<ExprNode*>& other) { 
                if (other.size() < 2) {
                    ctx->SetError("Join tail args error with any split");
                }
                return split;
            });

    // resolve "join"
    // illegal arg types
    fn_def = GetFnDef<int32_t, int32_t>(library, "join", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // prefer match non-variadic, prefer explicit match
    fn_def = GetFnDef<std::string, std::string>(library, "join", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);

    // prefer match non-variadic, allow placeholder
    fn_def = GetFnDef<std::string, int32_t>(library, "join", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);

    // prefer match non-variadic, but placeholder check failed
    fn_def = GetFnDef<std::string, int64_t>(library, "join", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // match variadic, prefer no-placeholder match
    fn_def = GetFnDef<std::string, bool, bool, bool>(library, "join", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);

    // match variadic, prefer no-placeholder match, buf impl logic failed
    fn_def = GetFnDef<std::string, std::string, std::string>(library, "join", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // match variadic with placeholder
    fn_def = GetFnDef<int16_t, std::string, std::string>(library, "join", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);
}


TEST_F(UDFRegistryTest, test_variadic_expr_udf_register_order) {
    UDFLibrary library;
    node::NodeManager nm;
    const node::FnDefNode* fn_def;

    // define "concat"
    library.RegisterExprUDF("concat")
        .variadic_args<>(
            [](UDFResolveContext* ctx, const std::vector<ExprNode*> other) {
                return ctx->node_manager()->MakeExprIdNode("concat");
            })

        .variadic_args<int32_t>(
            [](UDFResolveContext* ctx, ExprNode* x,
               const std::vector<ExprNode*> other) {
                if (other.size() > 2) {
                    ctx->SetError("Error");
                }
                return ctx->node_manager()->MakeExprIdNode("concat");
            });

    // resolve "concat"
    // prefer long non-variadic parameters part
    fn_def = GetFnDef<int32_t, std::string, std::string>(library, "concat", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);
    fn_def = GetFnDef<int32_t, bool, bool, bool>(library, "concat", &nm);
    ASSERT_TRUE(fn_def == nullptr);

    // empty variadic args
    fn_def = GetFnDef<>(library, "concat", &nm);
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDFDef);
}


TEST_F(UDFRegistryTest, test_external_udf_register) {
    UDFLibrary library;
    node::NodeManager nm;
    auto over = nm.MakeWindowDefNode("w");
    const node::ExternalFnDefNode* fn_def;

    // define "add"
    library.RegisterExternal("add")
        .allow_window(false)
        .allow_project(true)

        .args<AnyArg, AnyArg>(
            "add_any", reinterpret_cast<void*>(0x01))
        .returns<int32_t>()

        .args<double, double>(
            "add_double", reinterpret_cast<void*>(0x02))
        .returns<double>()

        .args<std::string, AnyArg, bool>(
            "add_string", reinterpret_cast<void*>(0x03))
        .returns<std::string>();

    // resolve "add"
    // match placeholder
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, int32_t>(library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("add_any", fn_def->function_name());
    ASSERT_EQ("int32", fn_def->ret_type()->GetName());

    // allow_window(false)
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, int32_t>(library, "add", &nm, over));
    ASSERT_TRUE(fn_def == nullptr);

    // match argument num
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, int32_t, int32_t>(library, "add", &nm));
    ASSERT_TRUE(fn_def == nullptr);

    // match explicit
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<double, double>(library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("add_double", fn_def->function_name());
    ASSERT_EQ("double", fn_def->ret_type()->GetName());

    // match with unknown input arg type
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, AnyArg>(library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("add_any", fn_def->function_name());
    ASSERT_EQ("int32", fn_def->ret_type()->GetName());

    // match different argument num
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<std::string, int64_t, bool>(library, "add", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("add_string", fn_def->function_name());
    ASSERT_EQ("string", fn_def->ret_type()->GetName());
}


TEST_F(UDFRegistryTest, test_variadic_external_udf_register) {
    UDFLibrary library;
    node::NodeManager nm;
    const node::ExternalFnDefNode* fn_def;
    
    // define "join"
    library.RegisterExternal("join")
        .args<std::string, std::string>("join2", nullptr)
        .returns<std::string>()

        .args<std::string, AnyArg>("join22", nullptr)
        .returns<std::string>()

        .variadic_args<std::string>("join_many", nullptr)
        .returns<std::string>()

        .variadic_args<AnyArg>("join_many2", nullptr)
        .returns<std::string>();

    // resolve "join"
    // prefer match non-variadic, prefer explicit match
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<std::string, std::string>(library, "join", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("join2", fn_def->function_name());
    ASSERT_EQ("string", fn_def->ret_type()->GetName());

    // prefer match non-variadic, allow placeholder
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<std::string, int32_t>(library, "join", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("join22", fn_def->function_name());

    // match variadic, prefer no-placeholder match
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<std::string, bool, bool, bool>(library, "join", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("join_many", fn_def->function_name());

    // match variadic with placeholder
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int16_t, std::string, std::string>(library, "join", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("join_many2", fn_def->function_name());
}


TEST_F(UDFRegistryTest, test_variadic_external_udf_register_order) {
    UDFLibrary library;
    node::NodeManager nm;
    const node::ExternalFnDefNode* fn_def;

    // define "concat"
    library.RegisterExternal("concat")
        .variadic_args<>("concat0", nullptr).returns<std::string>()
        .variadic_args<int32_t>("concat1", nullptr).returns<std::string>();

    // resolve "concat"
    // prefer long non-variadic parameters part
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<int32_t, std::string, std::string>(library, "concat", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("concat1", fn_def->function_name());
    ASSERT_EQ(1, fn_def->variadic_pos());

    // empty variadic args
    fn_def = dynamic_cast<const node::ExternalFnDefNode*>(
        GetFnDef<>(library, "concat", &nm));
    ASSERT_TRUE(fn_def != nullptr &&
        fn_def->GetType() == node::kExternalFnDef);
    ASSERT_EQ("concat0", fn_def->function_name());
    ASSERT_EQ(0, fn_def->variadic_pos());
}


TEST_F(UDFRegistryTest, test_simple_udaf_register) {
    UDFLibrary library;
    node::NodeManager nm;
    const node::UDAFDefNode* fn_def;

    library.RegisterExprUDF("add")
        .args<AnyArg, AnyArg>(
            [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
                auto res = ctx->node_manager()->MakeBinaryExprNode(
                    x, y, node::kFnOpAdd);
                res->SetOutputType(x->GetOutputType());
                return res;
            });

    library.RegisterExprUDF("identity")
        .args<AnyArg>(
            [](UDFResolveContext* ctx, ExprNode* x){
                return x;
            });

    library.RegisterSimpleUDAF("sum")
        .templates<int32_t, int32_t, int32_t>()
            .const_init(0)
            .update("add")
            .merge("add")
            .output("identity")
        .templates<float, double, double>()
            .const_init(0.0)
            .update("add")
            .merge("add")
            .output("identity")
        .finalize();

    fn_def = dynamic_cast<const node::UDAFDefNode*>(
        GetFnDef<int32_t>(library, "sum", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDAFDef);

    fn_def = dynamic_cast<const node::UDAFDefNode*>(
        GetFnDef<int32_t>(library, "sum", &nm));
    ASSERT_TRUE(fn_def != nullptr && fn_def->GetType() == node::kUDAFDef);

    fn_def = dynamic_cast<const node::UDAFDefNode*>(
        GetFnDef<std::string>(library, "sum", &nm));
    ASSERT_TRUE(fn_def == nullptr);

/*
    template <typename T>
    class MySetImpl {

    };


    template <typename T>
    void RegisterDistinctCount() {
        std::string arg_type = ArgTypeTrait<T>::to_string();

        library.RegisterSimpleUDAF("distinct_count")

            .templates<T, Opaque<sizeof(MySetImpl<T>), int64_t>>()
                .init("set_init_" + arg_type, MySetImpl<T>::Init)
                .update("set_insert_" + arg_type, MySetImpl<T>::Insert)
                .output("set_size_" + arg_type, MySetImpl<T>::Size)

            .finalize();
    }

    RegisterDistinctCount<int16_t>();
    RegisterDistinctCount<int32_t>();
    RegisterDistinctCount<int64_t>();
    RegisterDistinctCount<float>();
    RegisterDistinctCount<double>();
    RegisterDistinctCount<std::string>();*/
}

}  // namespace udf
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
