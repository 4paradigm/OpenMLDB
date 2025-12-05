/**
 * Copyright (c) 2023 4Paradigm Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/map_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "node/expr_node.h"
#include "node/type_node.h"
#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {

void DefaultUdfLibrary::InitMapUdfs() {
    RegisterCodeGenUdf("map")
        .variadic_args<>(
            // infer
            [](UdfResolveContext* ctx, const std::vector<node::ExprAttrNode>& arg_attrs,
               ExprAttrNode* out) -> base::Status {
                auto ret = node::MapType::InferMapType(ctx->node_manager(), arg_attrs);
                CHECK_TRUE(ret.ok(), common::kTypeError, ret.status().ToString());
                out->SetType(ret.value());
                out->SetNullable(true);
                return {};
            },
            // gen
            [](codegen::CodeGenContext* ctx, const std::vector<codegen::NativeValue>& args,
               const ExprAttrNode& return_info, codegen::NativeValue* out) -> base::Status {
                CHECK_TRUE(return_info.type()->IsMap(), common::kTypeError, "not a map type output");
                auto* map_type = return_info.type()->GetAsOrNull<node::MapType>();
                CHECK_TRUE(map_type != nullptr, common::kTypeError, "can not cast to MapType");

                ::llvm::Type* key_type = nullptr;
                ::llvm::Type* value_type = nullptr;
                CHECK_TRUE(codegen::GetLlvmType(ctx->GetModule(), map_type->key_type(), &key_type),
                           common::kCodegenError);
                CHECK_TRUE(codegen::GetLlvmType(ctx->GetModule(), map_type->value_type(), &value_type),
                           common::kCodegenError);
                codegen::MapIRBuilder builder(ctx->GetModule(), key_type, value_type);
                auto res = builder.Construct(ctx, args);
                if (res.ok()) {
                    *out = res.value();
                    return {};
                }
                return {common::kCodegenError, res.status().ToString()};
            })
        .doc(R"(
             @brief map(key1, value1, key2, value2, ...) -  Creates a map with the given key/value pairs.

             Example:

             @code{.sql}
                select map(1, '1', 2, '2');
                -- {1: "1", 2: "2"}
             @endcode

             @since 0.9.0
             )");

    RegisterCodeGenUdf("map_keys")
        .args<AnyArg>(
            [](UdfResolveContext* ctx, const ExprAttrNode& in, ExprAttrNode* out) -> base::Status {
                CHECK_TRUE(in.type()->IsMap(), common::kTypeError, "map_keys requires a map data type, got ",
                           in.type()->DebugString());

                auto map_type = in.type()->GetAsOrNull<node::MapType>();
                CHECK_TRUE(map_type != nullptr, common::kTypeError);

                out->SetType(ctx->node_manager()->MakeNode<node::TypeNode>(node::kArray, map_type->key_type()));
                out->SetNullable(true);
                return {};
            },
            [](codegen::CodeGenContext* ctx, codegen::NativeValue in, const node::ExprAttrNode& return_info,
               codegen::NativeValue* out) -> base::Status {
                const node::TypeNode* type = nullptr;
                CHECK_TRUE(codegen::GetFullType(ctx->node_manager(), in.GetType(), &type), common::kTypeError);
                auto map_type = type->GetAsOrNull<node::MapType>();
                CHECK_TRUE(map_type != nullptr, common::kTypeError);

                ::llvm::Type* key_type = nullptr;
                ::llvm::Type* value_type = nullptr;
                CHECK_TRUE(codegen::GetLlvmType(ctx->GetModule(), map_type->key_type(), &key_type),
                           common::kCodegenError);
                CHECK_TRUE(codegen::GetLlvmType(ctx->GetModule(), map_type->value_type(), &value_type),
                           common::kCodegenError);
                codegen::MapIRBuilder builder(ctx->GetModule(), key_type, value_type);

                auto res = builder.MapKeys(ctx, in);
                if (res.ok()) {
                    *out = res.value();
                    return {};
                }
                return {common::kCodegenError, res.status().ToString()};
            })
        .doc(R"(
             @brief map_keys(map) - Returns an unordered array containing the keys of the map.

             Example:

             @code{.sql}
                select map_keys(map(1, '2', 3, '4'));
                -- [1, 3]
             @endcode

             @since 0.9.0
             )");
}

}  // namespace udf
}  // namespace hybridse
