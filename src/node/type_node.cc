/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * type_node.cc
 *
 * Author: chenjing
 * Date: 2019/10/11
 *--------------------------------------------------------------------------
 **/
#include "node/type_node.h"

namespace fesql {
namespace node {

bool TypeNode::IsArithmetic() const { return IsInteger() || IsFloating(); }

bool TypeNode::IsInteger() const {
    return base_ == node::kInt16 || base_ == node::kInt32 ||
           base_ == node::kInt64;
}

bool TypeNode::IsFloating() const {
    return base_ == node::kFloat || base_ == node::kDouble;
}

bool TypeNode::IsGeneric() const { return !generics_.empty(); }

}  // namespace node
}  // namespace fesql
