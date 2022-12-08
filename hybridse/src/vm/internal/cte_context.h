/*
 * Copyright 2022 4Paradigm authors
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

// ================================================================================ //
// context abstraction for common table expressions (CTEs) lives in SQL with clauses
// ================================================================================ //

#ifndef HYBRIDSE_SRC_VM_INTERNAL_CTE_CONTEXT_H_
#define HYBRIDSE_SRC_VM_INTERNAL_CTE_CONTEXT_H_

#include <stack>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "base/fe_hash.h"
#include "base/fe_object.h"
#include "node/plan_node.h"

namespace hybridse {
namespace vm {
class PhysicalOpNode;

namespace internal {

// All the abstractions here refer to the non-recursive-cte, recursive-cte is not considered

// CTE rules
// 1. CTEs can be referenced inside the query expression that contains the  `WITH`  clause.
//    This means the CTEs can be referenced by the outermost query contains the `WITH` clause,
//    as well as subqueries inside the outermost query.
//
//    E.g:
//      WITH q1 AS (my_query)
//      SELECT *
//      FROM
//        (WITH q2 AS (SELECT * FROM q1),  # q1 resolves to my_query
//              q3 AS (SELECT * FROM q1),  # q1 resolves to my_query
//              q1 AS (SELECT * FROM q1),  # q1 (in the query) resolves to my_query
//              q4 AS (SELECT * FROM q1)   # q1 resolves to the WITH subquery on the previous line.
//          SELECT * FROM q1)              # q1 resolves to the third inner WITH subquery.
// 2. Each CTE in the same `WITH` clause must have a unique name.
// 3. A local CTE can overrides an outer CTE or table with the same name.

struct Closure;

// single CTE record
struct CTEEntry : public ::hybridse::base::FeBaseObject {
    CTEEntry() = delete;
    explicit CTEEntry(node::WithClauseEntryPlanNode* entry, Closure* c) ABSL_ATTRIBUTE_NONNULL()
        : node(entry), closure(c) {}
    ~CTEEntry() override {}

    // origial CTE info inside WITH clause
    node::WithClauseEntryPlanNode* node;

    // corresponding closure context for the WithClauseEntry
    Closure* closure;

    // transformed physical plan for the node, might be null since the tranformation
    // for WITH clause entry is lazy evaluated
    PhysicalOpNode* transformed_op = nullptr;

    // friend bool operator==(const CTEEntry& lhs, const CTEEntry& rhs) {
    //     return lhs.node == rhs.node;
    // }
};

// CTE Environment: Single level captured CTEs visible to a query
struct CTEEnv {
    // Empty Context
    CTEEnv() = default;

    // Copy Constructor
    CTEEnv(const CTEEnv&) = default;

    // No move constructor
    CTEEnv(CTEEnv&&) = delete;

    friend bool operator==(const CTEEnv& lhs, const CTEEnv& rhs) { return absl::c_equal(lhs.ctes, rhs.ctes); }

    absl::flat_hash_map<absl::string_view, CTEEntry*> ctes;
};

// All captured CTEs visible to a query
struct Closure : ::hybridse::base::FeBaseObject {
    Closure() = default;
    // outermost closure
    explicit Closure(const CTEEnv& c) : clu(c) { InitCache(); }

    // new closure from parent closure and captured CTEs current level
    Closure(Closure* p, const CTEEnv& c) : parent(p), clu(c) { InitCache(); }

    ~Closure() override {}

    friend bool operator==(const Closure& lhs, const Closure& rhs) {
        return base::GeneralPtrEq(lhs.parent, rhs.parent) && lhs.clu == rhs.clu &&
               absl::c_equal(lhs.cte_map, rhs.cte_map);
    }

    Closure* parent = nullptr;
    CTEEnv clu;

    // cached map to archive O(1) query
    absl::flat_hash_map<absl::string_view, std::stack<CTEEntry*>> cte_map;

 protected:
    void InitCache() {
        if (parent != nullptr) {
            cte_map = parent->cte_map;
        }

        for (auto cte_kv : clu.ctes) {
            cte_map[cte_kv.first].push(cte_kv.second);
        }
    }
};

}  // namespace internal
}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_INTERNAL_CTE_CONTEXT_H_
