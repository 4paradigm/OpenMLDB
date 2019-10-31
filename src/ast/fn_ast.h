/*
 * fn_ast.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AST_FN_AST_H_
#define AST_FN_AST_H_

#include <vector>
#include <string>

namespace fesql {
namespace ast {

enum FnNodeType {
    //primary
    kFnPrimaryBool = 1,
    kFnPrimaryInt16,
    kFnPrimaryInt32,
    kFnPrimaryInt64,
    kFnPrimaryFloat,
    kFnPrimaryDouble,

    kFnDef,
    kFnValue,
    kFnId,
    kFnAssignStmt,
    kFnReturnStmt,
    kFnExpr,
    kFnExprBinary,
    kFnExprUnary,
    kFnPara,
    kFnParaList
};

inline const std::string FnNodeName(const FnNodeType& type) {
    switch (type) {
        case kFnPrimaryBool:
            return "bool";
        case kFnPrimaryInt16:
            return "int16";
        case kFnPrimaryInt32:
            return "int32";
        case kFnPrimaryInt64:
            return "int64";
        case kFnPrimaryFloat:
            return "float";
        case kFnPrimaryDouble:
            return "double";
        case kFnDef:
            return "def";
        case kFnValue:
            return "value";
        case kFnId:
            return "id";
        case kFnAssignStmt:
            return "=";
        case kFnReturnStmt:
            return "return";
        case kFnExpr:
            return "expr";
        case kFnExprBinary:
            return "bexpr";
        case kFnExprUnary:
            return "uexpr";
        case kFnPara:
            return "para";
        case kFnParaList:
            return "plist";
        default:
            return  "unknown";
    }

}

enum FnOperator {
    kFnOpAdd,
    kFnOpMinus,
    kFnOpMulti,
    kFnOpDiv,
    kFnOpBracket,
    kFnOpNone
};

struct FnNode {
    FnNodeType type;
    std::vector<FnNode*> children;
    int32_t indent;
};

struct FnNodeInt16 {
    FnNodeType type = kFnPrimaryInt16;
    std::vector<FnNode*> children;
    int32_t indent;
    int16_t value;
};

struct FnNodeInt32 {
    FnNodeType type = kFnPrimaryInt32;
    std::vector<FnNode*> children;
    int32_t indent;
    int32_t value;
};

struct FnNodeInt64 {
    FnNodeType type = kFnPrimaryInt64;
    std::vector<FnNode*> children;
    int32_t indent;
    int32_t value;
};

struct FnNodeFloat {
    FnNodeType type = kFnPrimaryFloat;
    std::vector<FnNode*> children;
    int32_t indent;
    float value;
};

struct FnNodeDouble {
    FnNodeType type = kFnPrimaryFloat;
    std::vector<FnNode*> children;
    int32_t indent;
    float value;
};

struct FnNodeFnDef {
    FnNodeType type = kFnDef;
    std::vector<FnNode*> children;
    int32_t indent;
    std::string name;
    FnNodeType ret_type;
};

struct FnAssignNode {
    FnNodeType type;
    std::vector<FnNode*> children;
    int32_t indent;
    std::string name;
};

struct FnParaNode {
    FnNodeType type;
    std::vector<FnNode*> children;
    std::string name;
    FnNodeType para_type;
};

struct FnBinaryExpr {
    FnNodeType type;
    std::vector<FnNode*> children;
    FnOperator op;
};

struct FnUnaryExpr {
    FnNodeType type;
    std::vector<FnNode*> children;
    FnOperator op;
};

struct FnIdNode {
    FnNodeType type;
    std::vector<FnNode*> children;
    std::string name;
};

} // namespace of ast
} // namespace of fesql
#endif /* !AST_FN_AST_H_ */
