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
    kInsertStmt,
    kCmdStmt,
    kExpr,
    kResTarget,
    kTable,
    kWindowFunc,
    kWindowDef,
    kFrameBound,
    kFrames,
    kColumnDesc,
    kColumnIndex,
    kIndexKey,
    kIndexTs,
    kIndexVersion,
    kIndexTTL,
    kName,
    kConst,
    kLimit,
    kList,
    kOrderBy,

    kDesc,
    kAsc,

    kFrameRange,
    kFrameRows,

    kPreceding,
    kFollowing,
    kCurrent,

    kFn,
    kFnDef,
    kFnValue,
    kFnAssignStmt,
    kFnReturnStmt,
    kFnPara,
    kFnParaList,
    kFnList,
    kUnknow
};

enum ExprType {
    kExprBinary,
    kExprUnary,
    kExprIn,
    kExprFunc,
    kExprCase,
    kExprCast,
    kExprId,
    kExprColumnRef,
    kExprPrimary,
    kExprList,
    kExprAll,
    kExprUnknow
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

enum CmdType {
    kCmdCreateGroup,
    kCmdCreateDatabase,
    kCmdCreateTable,
    kCmdUseDatabase,
    kCmdShowDatabases,
    kCmdShowTables,
    kCmdDescTable,
    kCmdDropTable
};
/**
 * Planner:
 *  basic class for plan
 *
 */
enum PlanType {
    kPlanTypeCmd,
    kPlanTypeSelect,
    kPlanTypeCreate,
    kPlanTypeInsert,
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
    kAnalyserErrorUnSupportFunction,
    kCreateErrorUnSupportColumnType,
    kCreateErrorDuplicationColumnName,
    kCreateErrorDuplicationIndexName,

    kPlanErrorUnknow = 4001,
    kPlanErrorUnSupport,
    kPlanErrorNullNode,
    kPlanErrorQueryTreeIsEmpty,
    kPlanErrorTableRefIsEmpty,
    kPlanErrorQueryMultiTable,

    kExecuteErrorUnknow = 4001,
    kExecuteErrorUnSupport,
    kExecuteErrorNullNode,

    kCmdErrorUnknow = 5001,
    kCmdErrorUnSupport,
    kCmdErrorNullNode,
    kCmdErrorPathError,

    kRpcErrorUnknow = 6001,
    kRpcErrorConnection,
};
}  // namespace error

namespace base {
struct Status {
    Status() : code(0), msg("ok") {}
    Status(int32_t status_code, const std::string &msg_str)
        : code(status_code), msg(msg_str) {}
    int32_t code;
    std::string msg;
};
}  // namespace base

}  // namespace fesql

#endif  // SRC_NODE_NODE_ENUM_H_
