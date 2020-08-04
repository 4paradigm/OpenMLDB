/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_test.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
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
            auto arg_list = nm->MakeExprList();
            for (auto arg_expr : {args...}) {
                arg_list->AddChild(arg_expr);
            }
            node::ExprNode* output_expr = nullptr;
            node::ExprAnalysisContext analysis_ctx(nm, nullptr);
            auto status = library->Transform(state->name, arg_list, nullptr,
                                             &analysis_ctx, &output_expr);
            if (!status.isOK() || output_expr == nullptr) {
                LOG(WARNING) << status.msg;
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

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_TEST_H_
