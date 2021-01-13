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
#include "proto/fe_common.pb.h"
#include "proto/fe_type.pb.h"
namespace fesql {
namespace node {

const char SPACE_ST[] = "+-";
const char SPACE_ED[] = "";
const char OR_INDENT[] = "|  ";
const char INDENT[] = "  ";

enum SQLNodeType {
    // SQL
    kCreateStmt,
    kInsertStmt,
    kCmdStmt,
    kExplainStmt,
    kCreateIndexStmt,
    kExpr,
    kType,
    kNodeList,
    kResTarget,
    kTableRef,
    kQuery,
    kWindowFunc,
    kWindowDef,
    kFrameBound,
    kFrameExtent,
    kFrames,
    kColumnDesc,
    kColumnIndex,
    kIndexKey,
    kIndexTs,
    kIndexVersion,
    kIndexTTL,
    kIndexTTLType,
    kName,
    kConst,
    kLimit,
    kFn,
    kFnDef,
    kFnHeader,
    kFnValue,
    kFnIfElseBlock,
    kFnIfBlock,
    kFnElseBlock,
    kFnElifBlock,
    kFnForInBlock,
    kFnAssignStmt,
    kFnReturnStmt,
    kFnIfStmt,
    kFnElifStmt,
    kFnElseStmt,
    kFnForInStmt,
    kFnPara,
    kFnParaList,
    kFnList,
    kExternalFnDef,
    kUDFDef,
    kUDFByCodeGenDef,
    kUDAFDef,
    kLambdaDef,
    kPartitionMeta,
    kReplicaNum,
    kDistributions,
    kCreateSpStmt,
    kInputParameter,
    kPartitionNum,
    kUnknow
};

enum TableRefType {
    kRefTable,
    kRefQuery,
    kRefJoin,
};

enum QueryType {
    kQuerySelect,
    kQuerySub,
    kQueryUnion,
};
enum ExprType {
    kExprBinary,
    kExprUnary,
    kExprBetween,
    kExprCall,
    kExprCase,
    kExprWhen,
    kExprCast,
    kExprId,
    kExprColumnRef,
    kExprColumnId,
    kExprPrimary,
    kExprList,
    kExprForIn,
    kExprRange,
    kExprAll,
    kExprStruct,
    kExprQuery,
    kExprOrder,
    kExprGetField,
    kExprCond,
    kExprUnknow = 9999
};
// typedef fesql::type::Type DataType;
enum DataType {
    kBool,
    kInt16,
    kInt32,
    kInt64,
    kFloat,
    kDouble,
    kVarchar,
    kDate,
    kTimestamp,
    kList,
    kHour,
    kMinute,
    kSecond,
    kDay,
    kMap,
    kIterator,
    kInt8Ptr,
    kRow,
    kOpaque,
    kTuple,
    kVoid = 100,
    kNull = 101,
    kPlaceholder = 102
};

enum TimeUnit {
    kTimeUnitYear,
    kTimeUnitMonth,
    kTimeUnitWeek,
    kTimeUnitDay,
    kTimeUnitHour,
    kTimeUnitMinute,
    kTimeUnitSecond,
    kTimeUnitMilliSecond,
    kTimeUnitMicroSecond
};
enum FnOperator {
    kFnOpAdd,
    kFnOpMinus,
    kFnOpMulti,
    kFnOpDiv,
    kFnOpFDiv,
    kFnOpMod,
    kFnOpAnd,
    kFnOpOr,
    kFnOpXor,
    kFnOpNot,
    kFnOpEq,
    kFnOpNeq,
    kFnOpLt,
    kFnOpLe,
    kFnOpGt,
    kFnOpGe,
    kFnOpDot,
    kFnOpAt,
    kFnOpLike,
    kFnOpIn,
    kFnOpBracket,
    kFnOpIsNull,
    kFnOpNonNull,
    kFnOpNone
};

enum FrameType {
    kFrameRange,
    kFrameRows,
    kFrameRowsRange,
    kFrameRowsMergeRowsRange
};
enum BoundType {
    kPrecedingUnbound = 0,
    kPreceding,
    kCurrent,
    kFollowing,
    kFollowingUnbound,
};
enum JoinType {
    kJoinTypeFull,
    kJoinTypeLast,
    kJoinTypeLeft,
    kJoinTypeRight,
    kJoinTypeInner,
    kJoinTypeConcat,
};

enum UnionType { kUnionTypeDistinct, kUnionTypeAll };

enum CmdType {
    kCmdCreateGroup,
    kCmdCreateDatabase,
    kCmdSource,
    kCmdUseDatabase,
    kCmdShowDatabases,
    kCmdShowTables,
    kCmdDescTable,
    kCmdDropTable,
    kCmdCreateIndex,
    kCmdDropIndex,
    kCmdShowCreateSp,
    kCmdShowProcedures,
    kCmdDropSp,
    kCmdDropDatabase,
    kCmdExit
};
enum ExplainType {
    kExplainLogical,
    kExplainPhysical,
};
enum PlanType {
    kPlanTypeCmd,
    kPlanTypeFuncDef,
    kPlanTypeCreate,
    kPlanTypeInsert,
    kPlanTypeScan,
    kPlanTypeQuery,
    kPlanTypeLimit,
    kPlanTypeFilter,
    kPlanTypeTable,
    kPlanTypeJoin,
    kPlanTypeUnion,
    kPlanTypeSort,
    kPlanTypeGroup,
    kPlanTypeDistinct,
    kPlanTypeProject,
    kPlanTypeRename,
    kProjectList,
    kPlanTypeWindow,
    kProjectNode,
    kPlanTypeCreateSp,
    kUnknowPlan = 100,
};

enum TTLType {
    kAbsolute,
    kLatest,
};

// batch plan node type
enum BatchPlanNodeType { kBatchDataset, kBatchPartition, kBatchMap };

enum RoleType { kLeader, kFollower };

}  // namespace node
}  // namespace fesql

#endif  // SRC_NODE_NODE_ENUM_H_
