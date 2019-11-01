/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * node_enum.h
 *      
 * Author: chenjing
 * Date: 2019/10/29 
 *--------------------------------------------------------------------------
**/

#ifndef FESQL_NODE_ENUM_H
#define FESQL_NODE_ENUM_H

#include <string>
namespace fesql {
namespace node {

const std::string SPACE_ST = "+-";
const std::string SPACE_ED = "";
const std::string OR_INDENT = "|\t";
const std::string INDENT = " \t";
enum SQLNodeType {
    //SQL
    kSelectStmt = 0,
    kExpr,
    kResTarget,
    kTable,
    kFunc,
    kType,
    kWindowFunc,
    kWindowDef,
    kFrameBound,
    kFrames,
    kColumn,
    kConst,
    kLimit,
    kAll,
    kList,
    kOrderBy,

    kPrimary,

    kDesc,
    kAsc,

    kFrameRange,
    kFrameRows,

    kPreceding,
    kFollowing,
    kCurrent,

    //primary
    kFnPrimaryBool,
    kFnPrimaryInt16,
    kFnPrimaryInt32,
    kFnPrimaryInt64,
    kFnPrimaryFloat,
    kFnPrimaryDouble,

    // fn
    kFnDef,
    kFnValue,
    kFnId,
    kFnAssignStmt,
    kFnReturnStmt,
    kFnExpr,
    kFnExprBinary,
    kFnExprUnary,
    kFnPara,
    kFnParaList,
    kFnList,
    kUnknow
};

enum DataType {
    kTypeInt16,
    kTypeInt32,
    kTypeInt64,
    kTypeFloat,
    kTypeDouble,
    kTypeString,
    kTypeNull
};
enum FnOperator {
    kFnOpAdd,
    kFnOpMinus,
    kFnOpMulti,
    kFnOpDiv,
    kFnOpBracket,
    kFnOpNone
};

/**
 * Planner:
 *  basic class for plan
 *
 */
enum PlanType {
    kSelect,
    kProjectList,
    kProject,
    kScalarFunction,
    kOpExpr,
    kAggFunction,
    kAggWindowFunction,
    kUnknowPlan,
};

}
}

namespace fesql {
namespace error {
enum ErrorType {
    kSucess= 0,

    kNodeErrorUnknow = 1001,
    kNodeErrorMakeNodeFail,

    kParserErrorUnknow = 2001,
    kParserErrorSyntax,
    kParserErrorAbort,

    kAnalyserErrorUnknow = 3001,
    kAnalyserErrorUnSupport,
    kAnalyserErrorInitialize,
    kAnalyserErrorParserTreeEmpty,
    kAnalyserErrorFromListEmpty,
    kAnalyserErrorQueryMultiTable,
    kAnalyserErrorTableRefIsNull,
    kAnalyserErrorTableNotExist,
    kAnalyserErrorColumnNotExist,
    kAnalyserErrorSQLTypeNotSupport,

    kSystemErrorUnknow = 4001,
    kSystemErrorMemory,

    kServerErrorUnknow = 5001,
    kServerErrorConnection,
    kServerErrorSend
};
}
}
#endif //FESQL_NODE_ENUM_H
