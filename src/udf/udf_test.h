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

#ifndef SRC_UDF_UDF_TEST_H_
#define SRC_UDF_UDF_TEST_H_

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"

#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/ir_base_builder_test.h"
#include "codegen/type_ir_builder.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"
#include "udf/udf.h"
#include "vm/jit.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace udf {

template <typename Ret, typename... Args>
class UDFFunctionBuilderWithFullInfo;

struct UDFFunctionBuilderState {
    std::string name;
    std::vector<node::TypeNode*> arg_types;
    std::vector<int> arg_nullable;
    node::TypeNode* ret_type;
    node::NodeManager nm;
    UDFLibrary* library;
};

typedef std::unique_ptr<UDFFunctionBuilderState> BuilderStatePtr;

template <typename Ret, typename... Args>
class UDFFunctionBuilderWithFullInfo {
 public:
    explicit UDFFunctionBuilderWithFullInfo(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    auto& library(UDFLibrary* library) {
        state->library = library;
        return *this;
    }

    codegen::ModuleTestFunction<Ret, Args...> build();

 private:
    BuilderStatePtr state;
};

template <typename... Args>
class UDFFunctionBuilderWithArgs {
 public:
    explicit UDFFunctionBuilderWithArgs(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    auto& library(UDFLibrary* library) {
        state->library = library;
        return *this;
    }

    template <typename Ret>
    auto returns() {
        state->ret_type = DataTypeTrait<Ret>::to_type_node(&(state->nm));
        return UDFFunctionBuilderWithFullInfo<Ret, Args...>(std::move(state));
    }

 private:
    BuilderStatePtr state;
};

template <typename Ret>
class UDFFunctionBuilderWithRet {
 public:
    auto& library(UDFLibrary* library) {
        state->library = library;
        return *this;
    }

    template <typename... Args>
    auto args() {
        state->arg_types = {DataTypeTrait<Args>::to_type_node(&(state->nm))...};
        state->arg_nullable = {IsNullableTrait<Args>::value...};
        return UDFFunctionBuilderWithFullInfo<Ret, Args...>(std::move(state));
    }

 private:
    BuilderStatePtr state;
};

class UDFFunctionBuilder {
 public:
    explicit UDFFunctionBuilder(const std::string& name)
        : state(new UDFFunctionBuilderState()) {
        state->name = name;
    }

    auto& library(udf::UDFLibrary* library) {
        state->library = library;
        return *this;
    }

    template <typename... Args>
    auto args() {
        state->arg_types = {DataTypeTrait<Args>::to_type_node(&(state->nm))...};
        state->arg_nullable = {IsNullableTrait<Args>::value...};
        return UDFFunctionBuilderWithArgs<Args...>(std::move(state));
    }

    template <typename Ret>
    auto returns() {
        state->ret_type = DataTypeTrait<Ret>::to_type_node(&(state->nm));
        return UDFFunctionBuilderWithRet<Ret>(std::move(state));
    }

 private:
    BuilderStatePtr state;
};

template <typename Ret, typename... Args>
codegen::ModuleTestFunction<Ret, Args...>
UDFFunctionBuilderWithFullInfo<Ret, Args...>::build() {
    UDFLibrary* library;
    if (state->library != nullptr) {
        library = state->library;
    } else {
        library = DefaultUDFLibrary::get();
    }
    return codegen::BuildExprFunction<Ret, Args...>(
        library,
        [this, library](
            node::NodeManager* nm,
            typename std::pair<Args, node::ExprNode*>::second_type... args)
            -> node::ExprNode* {
            // resolve udf call
            std::vector<node::ExprNode*> arg_vec = {args...};
            node::ExprNode* output_expr = nullptr;
            auto status =
                library->Transform(state->name, arg_vec, nm, &output_expr);
            if (!status.isOK() || output_expr == nullptr) {
                LOG(WARNING) << status;
                return nullptr;
            }
            return output_expr;
        });
}

template <typename T>
struct EqualValChecker {
    static void check(const T& expect, const T& output) {
        ASSERT_EQ(expect, output);
    }
};

template <>
struct EqualValChecker<float> {
    static void check(const float& expect, const float& output) {
        if (expect != expect) {
            bool is_nan = output != output;
            ASSERT_TRUE(is_nan);
        } else {
            ASSERT_FLOAT_EQ(expect, output);
        }
    }
};

template <>
struct EqualValChecker<double> {
    static void check(const double& expect, const double& output) {
        if (expect != expect) {
            bool is_nan = output != output;
            ASSERT_TRUE(is_nan);
        } else {
            ASSERT_FLOAT_EQ(expect, output);
        }
    }
};

template <typename T>
codec::ListRef<T> MakeList(const std::initializer_list<T>& vec) {
    codec::ArrayListV<T>* list =
        new codec::ArrayListV<T>(new std::vector<T>(vec));
    codec::ListRef<T> list_ref;
    list_ref.list = reinterpret_cast<int8_t*>(list);
    return list_ref;
}

codec::ListRef<bool> MakeBoolList(const std::initializer_list<int>& vec) {
    codec::BoolArrayListV* list =
        new codec::BoolArrayListV(new std::vector<int>(vec));
    codec::ListRef<bool> list_ref;
    list_ref.list = reinterpret_cast<int8_t*>(list);
    return list_ref;
}

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_TEST_H_
