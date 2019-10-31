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

#include "node/emun.h"
#include "node/sql_node.h"
#include <vector>
#include <string>

namespace fesql {
namespace node {

inline const std::string FnNodeName(const SQLNodeType &type) {
    switch (type) {
        case kFnPrimaryBool:return "bool";
        case kFnPrimaryInt16:return "int16";
        case kFnPrimaryInt32:return "int32";
        case kFnPrimaryInt64:return "int64";
        case kFnPrimaryFloat:return "float";
        case kFnPrimaryDouble:return "double";
        case kFnDef:return "def";
        case kFnValue:return "value";
        case kFnId:return "id";
        case kFnAssignStmt:return "=";
        case kFnReturnStmt:return "return";
        case kFnExpr:return "expr";
        case kFnExprBinary:return "bexpr";
        case kFnExprUnary:return "uexpr";
        case kFnPara:return "para";
        case kFnParaList:return "plist";
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

class FnNode : public SQLNode {
public:
    FnNode() : SQLNode(kFunc, 0, 0), indent(0) {};
    FnNode(SQLNodeType type) : SQLNode(type, 0, 0), indent(0) {};
public:
    SQLNodeType type;
    std::vector<FnNode *> children;
    int32_t indent;
};

class FnNodeInt16 : public FnNode {
public:
    FnNodeInt16() : FnNode(kFnPrimaryInt16) {};
public:
    int16_t value;
};

class FnNodeInt32 : public FnNode {
public:
    FnNodeInt32() : FnNode(kFnPrimaryInt32) {};
public:
    int32_t value;
};

class FnNodeInt64 : public FnNode {
public:
    FnNodeInt64() : FnNode(kFnPrimaryInt64){};
public:
    int32_t value;
};

class FnNodeFloat : public FnNode {
public:
    FnNodeFloat() : FnNode(kFnPrimaryFloat) {};
public:
    float value;
};

class FnNodeDouble : public FnNode {
public:
    FnNodeDouble() : FnNode(kFnPrimaryDouble) {};
public:
    float value;
};

class FnNodeFnDef : public FnNode {
public:
    FnNodeFnDef() : FnNode(kFnDef){};
public:
    char *name;
    SQLNodeType ret_type;
};

class FnAssignNode : public FnNode {
public:
    FnAssignNode() : FnNode(kFnAssignStmt){};
public:
    char *name;
};

class FnParaNode : public FnNode {
public:
    FnParaNode() : FnNode(kFnPara){};
public:
    char *name;
    SQLNodeType para_type;
};

class FnBinaryExpr : public FnNode {
public:
    FnBinaryExpr() : FnNode(kFnExprBinary) {};
public:
    FnOperator op;
};

class FnUnaryExpr : public FnNode {
public:
    FnUnaryExpr() : FnNode(kFnExprUnary){};
public:
    FnOperator op;
};

class FnIdNode : public FnNode {
public:
    FnIdNode() : FnNode(kFnId) {};
public:
    char *name;
};

} // namespace of ast
} // namespace of fesql
#endif /* !AST_FN_AST_H_ */
