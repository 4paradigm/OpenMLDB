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

#ifndef HYBRIDSE_INCLUDE_NODE_NODE_ENUM_H_
#define HYBRIDSE_INCLUDE_NODE_NODE_ENUM_H_

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
    kStorageMode,
    kCreateSpStmt,
    kInputParameter,
    kPartitionNum,
    kSelectIntoStmt,
    kLoadDataStmt,
    kDeployStmt,
    kSetStmt,
    kDeleteStmt,
    kCreateFunctionStmt,
    kDynamicUdfFnDef,
    kDynamicUdafFnDef,  // deprecated
    kWithClauseEntry,
    kUnknow = -1
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
    kExprUnknow = -1,
    kExprBinary = 0,
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
    kExprIn,
    kExprEscaped,
    kExprArray,
    kExprFake,  // not a real one
    kExprLast = kExprFake,
};

// typedef hybridse::type::Type DataType;
// TODO(ace): separate DataType into two group
//   - group 1: bool ~ list, map ~ array: those types are built in codegen
//   - group2: hour/minute/second/day, only appear in plan node level
enum DataType {
    kBool = 0,
    kInt16,
    kInt32,
    kInt64,
    kFloat,
    kDouble,
    kVarchar,
    kDate,
    kTimestamp,
    kList,  // dynamic sized, same element type, not nullable. usually ref to column ref or subquery
    kHour,
    kMinute,
    kSecond,
    kDay,
    kMap,
    kIterator,
    kInt8Ptr,
    kRow,
    kOpaque,
    kTuple,         // heterogeneous element type, fixed size
    kArray,         // fixed size. In SQL: [1, 2, 3] or ARRAY<int>[1, 2, 3]
    kDataTypeFake,  // not a data type, for testing purpose only
    kLastDataType = kDataTypeFake,
    // the tree type are not moved above kLastDataType for compatibility
    // it may necessary to do it in the further
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
    kFnOpAdd,         // "+"
    kFnOpMinus,       // "-"
    kFnOpMulti,       // "*"
    kFnOpDiv,         // "DIV", integer division
    kFnOpFDiv,        // "/", float division
    kFnOpMod,         // "%"
    kFnOpAnd,         // "AND", logical
    kFnOpOr,          // "OR" , logical
    kFnOpXor,         // "XOR", logical
    kFnOpNot,         // "NOT", logical
    kFnOpEq,          // "="
    kFnOpNeq,         // "!="
    kFnOpLt,          // "<"
    kFnOpLe,          // "<="
    kFnOpGt,          // ">"
    kFnOpGe,          // ">="
    kFnOpDot,         // "."
    kFnOpAt,          // "[]"
    kFnOpLike,        // "LIKE"
    kFnOpILike,       // "ILIKE"
    kFnOpRLike,       // "RLIKE"
    kFnOpIn,          // "IN"
    kFnOpBracket,     // "()"
    kFnOpIsNull,      // "is_null"
    kFnOpNonNull,     // "" a helper op for compile to ignore null check
    kFnOpNone,        // "NONE"
    kFnOpBitwiseAnd,  // "&"
    kFnOpBitwiseOr,   // "|"
    kFnOpBitwiseXor,  // "^"
    kFnOpBitwiseNot,  // "~"
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
    kCmdCreateDatabase = 0,
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
    kCmdExit,
    kCmdShowDeployment,
    kCmdShowDeployments,
    kCmdDropDeployment,
    kCmdShowJobs,
    kCmdShowJob,
    kCmdStopJob,
    kCmdShowGlobalVariables,
    kCmdShowSessionVariables,
    kCmdShowComponents,
    kCmdShowTableStatus,
    kCmdShowFunctions,
    kCmdDropFunction,
    kCmdShowJobLog,
    kCmdFake,  // not a real cmd, for testing purpose only
    kLastCmd = kCmdFake,
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
    kPlanTypeSelectInto,
    kPlanTypeLoadData,
    kPlanTypeDeploy,
    kPlanTypeSet,
    kPlanTypeDelete,
    kPlanTypeCreateFunction,
    kPlanTypeWithClauseEntry,
    kUnknowPlan = -1,
};

enum TTLType {
    kAbsolute,
    kLatest,
};

enum VariableScope {
    kGlobalSystemVariable,
    kSessionSystemVariable,
};

enum StorageMode {
    kUnknown = 0,
    kMemory = 1,
    kSSD = 2,
    kHDD = 3,
};

// batch plan node type
enum BatchPlanNodeType { kBatchDataset, kBatchPartition, kBatchMap };

enum RoleType { kLeader, kFollower };

}  // namespace node
}  // namespace hybridse

#endif  // HYBRIDSE_INCLUDE_NODE_NODE_ENUM_H_
