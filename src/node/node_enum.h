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
    kCreateStmt,
    kExpr,
    kResTarget,
    kTable,
    kFunc,
    kType,
    kWindowFunc,
    kWindowDef,
    kFrameBound,
    kFrames,
    kColumnRef,
    kColumnDesc,
    kColumnIndex,
    kIndexKey,
    kIndexTs,
    kIndexVersion,
    kIndexTTL,
    kName,
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
    kTypeBool,
    kTypeInt16,
    kTypeInt32,
    kTypeInt64,
    kTypeFloat,
    kTypeDouble,
    kTypeString,
    kTypeTimestamp,
    kTypeHour,
    kTypeDay,
    kTypeMinute,
    kTypeSecond,
    kTypeNull
};

enum TimeUnit {
    kTimeUnitHour,
    kTimeUnitDay,
    kTimeUnitMinute,
    kTimeUnitSecond,
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
    kPlanTypeSelect,
    kPlanTypeCreate,
    kPlanTypeScan,
    kPlanTypeLimit,
    kPlanTypeFilter,
    kProjectList,
    kProject,
    kScalarFunction,
    kOpExpr,
    kAggFunction,
    kAggWindowFunction,
    kUnknowPlan,

    kScanTypeSeqScan,
    kScanTypeIndexScan,
};

}
}

namespace fesql {
namespace error {
enum ErrorType {
    kSucess = 0,

    kNodeErrorUnknow = 1001,
    kNodeErrorMakeNodeFail,

    kParserErrorUnknow = 2001,
    kParserErrorSyntax,
    kParserErrorAbort,

    kAnalyserErrorUnknow = 3001,
    kAnalyserErrorUnSupport,
    kAnalyserErrorSQLTypeNotSupport,
    kAnalyserErrorInitialize,
    kAnalyserErrorParserTreeEmpty,
    kAnalyserErrorFromListEmpty,
    kAnalyserErrorQueryMultiTable,
    kAnalyserErrorTableRefIsNull,
    kAnalyserErrorTableNotExist,
    kAnalyserErrorTableAlreadyExist,
    kAnalyserErrorColumnNameIsEmpty,
    kAnalyserErrorColumnNotExist,
    kAnalyserErrorTargetIsNull,
    kAnalyserErrorGlobalAggFunction,
    kCreateErrorUnSupportColumnType,
    kCreateErrorDuplicationColumnName,
    kCreateErrorDuplicationIndexName,

    kPlanErrorUnknow = 4001,
    kPlanErrorUnSupport,
    kPlanErrorNullNode,
    kPlanErrorTableRefIsEmpty,
    kPlanErrorQueryMultiTable,

    kSystemErrorUnknow = 5001,
    kSystemErrorMemory,

    kServerErrorUnknow = 6001,
    kServerErrorConnection,
    kServerErrorSend
};
}
}
#endif //FESQL_NODE_ENUM_H
