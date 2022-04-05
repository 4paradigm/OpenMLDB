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

#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

using openmldb::base::Date;
using hybridse::codec::ListRef;
using openmldb::base::StringRef;
using openmldb::base::Timestamp;

namespace hybridse {
namespace udf {

template <class V>
void AtList(::hybridse::codec::ListRef<V>* list_ref, int64_t pos, V* v,
            bool* is_null) {
    if (pos < 0) {
        *is_null = true;
        *v = V(DataTypeTrait<V>::zero_value());
        return;
    }
    auto list = (codec::ListV<V>*)(list_ref->list);
    auto column = dynamic_cast<codec::WrapListImpl<V, codec::Row>*>(list);
    if (column != nullptr) {
        auto row = column->root()->At(pos);
        if (row.empty()) {
            *is_null = true;
            *v = V(DataTypeTrait<V>::zero_value());
        } else {
            column->GetField(row, v, is_null);
        }
    } else {
        *is_null = false;
        *v = list->At(pos);
    }
}

node::ExprNode* BuildAt(UdfResolveContext* ctx, ExprNode* input, ExprNode* idx,
                        ExprNode* default_val) {
    auto input_type = input->GetOutputType();
    if (input_type->base() != node::kList) {
        ctx->SetError("Input type is not list: " + input_type->GetName());
        return nullptr;
    }
    if (input_type->GetGenericType(0)->IsGeneric()) {
        ctx->SetError("Do not support generic element type: " +
                      input_type->GetName());
        return nullptr;
    }
    if (default_val != nullptr) {
        auto default_type = default_val->GetOutputType();
        if (default_type->base() != node::kNull) {
            if (node::TypeEquals(default_type, input_type->GetGenericType(0))) {
                ctx->SetError(
                    "Default value type must be same with input element "
                    "type: " +
                    default_type->GetName());
                return nullptr;
            }
        }
    }
    auto nm = ctx->node_manager();
    if (idx->GetOutputType() == nullptr ||
        idx->GetOutputType()->base() != node::kInt64) {
        idx = nm->MakeCastNode(node::kInt64, idx);
    }
    auto res = nm->MakeFuncNode("at", {input, idx}, nullptr);
    if (default_val != nullptr) {
        res = nm->MakeFuncNode("if_null", {res, default_val}, nullptr);
    }
    return res;
}

template <typename V>
void RegisterBaseListAt(UdfLibrary* lib) {
    lib->RegisterExternal("at")
        .doc(R"(
            @brief Returns the value of expression from the offset-th row of the ordered partition.

            @param offset The number of rows forward from the current row from which to obtain the value.

            Example:

            |value|
            |--|
            |0|
            |1|
            |2|
            |3|
            |4|
            @code{.sql}
                SELECT at(value, 3) OVER w;
                -- output 3
            @endcode
        )")
        .args<codec::ListRef<V>, int64_t>(reinterpret_cast<void*>(AtList<V>))
        .return_by_arg(true)
        .template returns<Nullable<V>>();
}

void DefaultUdfLibrary::InitWindowFunctions() {
    // basic at impl for <list<V>, int32>
    RegisterBaseListAt<bool>(this);
    RegisterBaseListAt<int16_t>(this);
    RegisterBaseListAt<int32_t>(this);
    RegisterBaseListAt<int64_t>(this);
    RegisterBaseListAt<float>(this);
    RegisterBaseListAt<double>(this);
    RegisterBaseListAt<Date>(this);
    RegisterBaseListAt<Timestamp>(this);
    RegisterBaseListAt<StringRef>(this);

    // general at
    RegisterExprUdf("at").list_argument_at(0).args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* input, ExprNode* idx) {
            return BuildAt(ctx, input, idx, nullptr);
        });

    RegisterAlias("lag", "at");
    RegisterExprUdf("first_value")
        .list_argument_at(0)
        .args<AnyArg>([](UdfResolveContext* ctx, ExprNode* input) {
            return BuildAt(ctx, input, ctx->node_manager()->MakeConstNode(0),
                           nullptr);
        })
        .doc(
            R"(@brief Returns the value of expr from the first row of the window frame.

        @since 0.1.0)");
}

}  // namespace udf
}  // namespace hybridse
