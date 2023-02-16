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

#include "absl/cleanup/cleanup.h"
#include "udf/containers.h"
#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

using openmldb::base::Date;
using hybridse::codec::ListRef;
using openmldb::base::StringRef;
using openmldb::base::Timestamp;

namespace hybridse {
namespace udf {

template <class V>
void AtList(::hybridse::codec::ListRef<V>* list_ref, int64_t pos, V* v, bool* is_null) {
    if (pos < 0) {
        *is_null = true;
        *v = static_cast<V>(DataTypeTrait<V>::zero_value());
        return;
    }
    auto list = reinterpret_cast<codec::ListV<V>*>(list_ref->list);
    auto column = dynamic_cast<codec::WrapListImpl<V, codec::Row>*>(list);
    if (column != nullptr) {
        auto row = column->root()->At(pos);
        if (row.empty()) {
            *is_null = true;
            *v = static_cast<V>(DataTypeTrait<V>::zero_value());
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

template <typename T>
struct NthValueWhere {
    // C type for value
    using CType = typename DataTypeTrait<T>::CCallArgType;

    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        helper.library()
            ->RegisterUdafTemplate<Impl>(helper.name())
            .doc(helper.GetDoc())
            .template args_in<int16_t, int32_t, int64_t>();
    }

    template <typename NthType>
    struct Impl {
        void operator()(UdafRegistryHelper& helper) {  // NOLINT
            std::string prefix = absl::StrCat(helper.name(), "_", DataTypeTrait<T>::to_string(), "_",
                                              DataTypeTrait<NthType>::to_string());
            helper.templates<Nullable<T>, Opaque<ContainerT>, Nullable<T>, NthType, Nullable<bool>>()
                .init(prefix + "_init", Init)
                .update(prefix + "_update", Update)
                .output(prefix + "_output", reinterpret_cast<void*>(Output), true);
        }

     private:
        struct ContainerT {
            ContainerT() : nth(0), cur_pos(0) {}

            // given nth value, be negative or positive
            NthType nth;

            // current iterator pos (start from 1), non-negative
            NthType cur_pos;

            // saved value list, only updated if `nth` > 0
            std::vector<std::pair<T, bool>> data;
        };

        // (nth idx, [(value, value_is_null)])
        // using ContainerT = std::pair<NthType, std::vector<std::pair<T, bool>>>;

        static void Init(ContainerT* ctr) { new (ctr) ContainerT(); }

        static ContainerT* Update(ContainerT* ctr, CType value, bool value_is_null, NthType nth, bool cond,
                                  bool cond_is_null) {
            // by default, update is iterated from window end to window start
            if (ctr->nth == 0) {
                // update only once, we treat nth as constant
                ctr->nth = nth;
            }
            if (cond && !cond_is_null) {
                if (ctr->nth > 0) {
                    // nth from window start
                    ctr->data.emplace_back(container::ContainerStorageTypeTrait<T>::to_stored_value(value),
                                           value_is_null);
                } else {
                    // nth from window end
                    ctr->cur_pos++;
                    if (ctr->cur_pos + ctr->nth == 0) {
                        // reaches nth
                        ctr->data.emplace_back(container::ContainerStorageTypeTrait<T>::to_stored_value(value),
                                               value_is_null);
                    }
                }
            }
            return ctr;
        }

        static void Output(ContainerT* ctr, T* value, bool* is_null) {
            absl::Cleanup clean = [ctr]() { ctr->~ContainerT(); };

            if (ctr->nth == 0) {
                *is_null = true;
                return;
            }

            if (ctr->nth > 0) {
                // count from window start to window end
                size_t sz = ctr->data.size();
                if (sz < ctr->nth) {
                    *is_null = true;
                    return;
                }
                *value = ctr->data[sz - ctr->nth].first;
                *is_null = ctr->data[sz - ctr->nth].second;
                return;
            }

            if (ctr->data.empty()) {
                *is_null = true;
                return;
            }

            // count from window end
            *value = ctr->data[0].first;
            *is_null = ctr->data[0].second;
        }
    };
};

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
        .doc(R"(
        @brief Returns the value of expr from the latest row (last row) of the window frame.

        Example:

        @code{.sql}
        select id, gp, ts, first_value(ts) over w as agg from t1
        window w as (partition by gp order by ts rows between 3 preceding and current row);
        @endcode

        | id | gp | ts | agg |
        | -- | -- | -- | --- |
        | 1  | 100 | 98 | 98 |
        | 2  | 100 | 99 | 99 |
        | 3  | 100 | 100 | 100 |

        @since 0.1.0)");

    RegisterUdafTemplate<NthValueWhere>("nth_value_where")
        .doc(R"(
        @brief Returns the value of expr from the idx th row matches the condition.

        @param value Expr of the matched row
        @param idx Idx th matched row (start from 1 or -1). If positive, count from first row of window; if negative, count from last row of window; 0 is invalid, results NULL.
        @cond Match expression of the row.

        Example:

        @code{.sql}
          select col1, cond, gp, nth_value_where(col1, 2, cond) over (partition by gp order by col1 rows between 10 preceding and current row) as agg from t1;
        @endcode

        | col1 | cond | gp |  agg |
        | ---- | ---  | -- |  --- |
        | 1    | true |  100 | NULL |
        | 2    | false | 100 | NULL |
        | 3    | NULL |  100 | NULL |
        | 4    | true |  100 | 4    |

        @since 0.8.0
             )")
        // value type
        .args_in<bool, int16_t, int32_t, int64_t, float, double, StringRef, Timestamp, Date>();
}

}  // namespace udf
}  // namespace hybridse
