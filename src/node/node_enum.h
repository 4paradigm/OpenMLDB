/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * node_enum.h
 *
 * Author: chenjing
 * Date: 2019/10/29
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_NODE_NODE_ENUM_H_
#define SRC_NODE_NODE_ENUM_H_

#include <string>
namespace fesql {
namespace node {

const char SPACE_ST[] = "+-";
const char SPACE_ED[] = "";
const char OR_INDENT[] = "|\t";
const char INDENT[] = " \t";
enum SQLNodeType {
  // SQL
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

}  // namespace node
}  // namespace fesql

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
}  // namespace error
}  // namespace fesql
#endif  // SRC_NODE_NODE_ENUM_H_
