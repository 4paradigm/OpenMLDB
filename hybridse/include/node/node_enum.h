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

#ifndef INCLUDE_NODE_NODE_ENUM_H_
#define INCLUDE_NODE_NODE_ENUM_H_

#include <string>
#include "proto/fe_common.pb.h"
#include "proto/fe_type.pb.h"
namespace hybridse {
namespace node {

const char SPACE_ST[] = "+-";
const char SPACE_ED[] = "";
const char OR_INDENT[] = "|  ";
const char INDENT[] = "  ";

enum SqlNodeType {
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
    kUdfDef,
    kUdfByCodeGenDef,
    kUdafDef,
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
    kExprParameter,
    kExprList,
    kExprForIn,
    kExprRange,
    kExprAll,
    kExprStruct,
    kExprQuery,
    kExprOrder,
    kExprOrderExpression,
    kExprGetField,
    kExprCond,
    kExprUnknow = 9999
};
// typedef hybridse::type::Type DataType;
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
    kOpenPreceding,
    kCurrent,
    kOpenFollowing,
    kFollowing,
    kFollowingUnbound,
};
enum ExcludeType {
    kNonExclude,
    kExcludeCurrentTime,
};
enum JoinType {
    kJoinTypeFull,
    kJoinTypeLast,
    kJoinTypeLeft,
    kJoinTypeRight,
    kJoinTypeInner,
    kJoinTypeConcat,
    kJoinTypeComma
};

enum UnionType { kUnionTypeDistinct, kUnionTypeAll };

enum CmdType {
    kCmdCreateDatabase,
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
    kPlanTypeExplain,
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
    kPlanTypeCreateIndex,
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
}  // namespace hybridse

#endif  // INCLUDE_NODE_NODE_ENUM_H_
