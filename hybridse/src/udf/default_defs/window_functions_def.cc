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
    auto list = reinterpret_cast<codec::ListV<V>*>(list_ref->list);
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
            if (!node::TypeEquals(default_type, input_type->GetGenericType(0))) {
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
void RegisterBaseListLag(UdfLibrary* lib) {
    lib->RegisterExternal("lag")
        .doc(R"(
            @brief Returns value evaluated at the row that is offset rows before the current row within the partition.
            Offset is evaluated with respect to the current row

            Note: This function equals the `at()` function.

            The offset in window is `nth_value()`, not `lag()/at()`. The old `at()`(version < 0.5.0) is start
            from the last row of window(may not be the current row), it's more like `nth_value()`

            @param offset The number of rows forwarded from the current row, must not negative

            Example:

            |c1|c2|
            |--|--|
            |0 | 1|
            |1 | 1|
            |2 | 2|
            |3 | 2|
            |4 | 2|
            @code{.sql}
                SELECT lag(c1, 1) over w as co from t1 window w as(partition by c2 order by c1 rows between unbounded preceding and current row);
                -- output
                -- | co |
                -- |----|
                -- |NULL|
                -- |0   |
                -- |NULL|
                -- |2   |
                -- |3   |
                SELECT at(c1, 1) over w as co from t1 window w as(partition by c2 order by c1 rows between unbounded preceding and current row);
                -- output
                -- | co |
                -- |----|
                -- |NULL|
                -- |0   |
                -- |NULL|
                -- |2   |
                -- |3   |
            @endcode

        )")
        .args<codec::ListRef<V>, int64_t>(reinterpret_cast<void*>(AtList<V>))
        .return_by_arg(true)
        .template returns<Nullable<V>>();
}

void DefaultUdfLibrary::InitWindowFunctions() {
    // basic at impl for <list<V>, int32>
    RegisterBaseListLag<bool>(this);
    RegisterBaseListLag<int16_t>(this);
    RegisterBaseListLag<int32_t>(this);
    RegisterBaseListLag<int64_t>(this);
    RegisterBaseListLag<float>(this);
    RegisterBaseListLag<double>(this);
    RegisterBaseListLag<Date>(this);
    RegisterBaseListLag<Timestamp>(this);
    RegisterBaseListLag<StringRef>(this);

    // general lag
    RegisterExprUdf("lag").list_argument_at(0).args<AnyArg, AnyArg>(
        [](UdfResolveContext* ctx, ExprNode* input, ExprNode* idx) {
            return BuildAt(ctx, input, idx, nullptr);
        });

    RegisterAlias("at", "lag");
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
