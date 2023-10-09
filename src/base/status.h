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

#ifndef SRC_BASE_STATUS_H_
#define SRC_BASE_STATUS_H_

#include <string>

#include "base/slice.h"
#include "version.h"  // NOLINT

namespace openmldb {
namespace base {

inline const std::string NOTICE_URL = "https://openmldb.ai/docs/zh/v" + std::to_string(OPENMLDB_VERSION_MAJOR) + "." +
    std::to_string(OPENMLDB_VERSION_MINOR) + "/openmldb_sql/notice.html";

enum ReturnCode {
    kError = -1,
    // TODO(zekai): Add some notes, it is hard to use these error codes
    kOk = 0,
    kTableIsNotExist = 100,
    kTableAlreadyExists = 101,
    kTableIsLeader = 102,
    kTableIsFollower = 103,
    kTableIsLoading = 104,
    kTableStatusIsNotKnormal = 105,
    kTableStatusIsKmakingsnapshot = 106,
    kTableStatusIsNotKsnapshotpaused = 107,
    kIdxNameNotFound = 108,
    kKeyNotFound = 109,
    kReplicatorIsNotExist = 110,
    kSnapshotIsNotExist = 111,
    kTtlTypeMismatch = 112,
    kTsMustBeGreaterThanZero = 114,
    kInvalidDimensionParameter = 115,
    kPutFailed = 116,
    kStLessThanEt = 117,
    kReacheTheScanMaxBytesSize = 118,
    kReplicaEndpointAlreadyExists = 119,
    kFailToAddReplicaEndpoint = 120,
    kReplicatorRoleIsNotLeader = 121,
    kFailToAppendEntriesToReplicator = 122,
    kFileReceiverInitFailed = 123,
    kCannotFindReceiver = 124,
    kBlockIdMismatch = 125,
    kReceiveDataError = 126,
    kWriteDataFailed = 127,
    kSnapshotIsSending = 128,
    kTableMetaIsIllegal = 129,
    kTableDbPathIsNotExist = 130,
    kCreateTableFailed = 131,
    kTtlIsGreaterThanConfValue = 132,
    kCannotUpdateTtlBetweenZeroAndNonzero = 133,
    kNoFollower = 134,
    kInvalidConcurrency = 135,
    kDeleteFailed = 136,
    kTsNameNotFound = 137,
    kFailToGetDbRootPath = 138,
    kFailToGetRecycleRootPath = 139,
    kUpdateFailed = 140,
    kIndexAlreadyExists = 141,
    kDeleteIndexFailed = 142,
    kAddIndexFailed = 143,
    kTableTypeMismatch = 145,
    kDumpIndexDataFailed = 146,
    kSnapshotRecycled = 147,
    kQueryFailed = 148,
    kPutBadFormat = 149,
    kUnkownTableType = 150,
    kColNameNotFound = 151,
    kEncodeError = 152,
    kAddTypeToColumnDescFailed = 153,
    kUseNameIsFalse = 154,
    kServerNameNotFound = 155,
    kSdkEndpointDuplicate = 156,
    kProcedureAlreadyExists = 157,
    kProcedureNotFound = 158,
    kCreateFunctionFailed = 159,
    kExceedMaxMemory = 160,
    kInvalidArgs = 161,
    kCheckIndexFailed = 162,
    kNameserverIsNotLeader = 300,
    kAutoFailoverIsEnabled = 301,
    kEndpointIsNotExist = 302,
    kTabletIsNotHealthy = 303,
    kSetZkFailed = 304,
    kCreateOpFailed = 305,
    kAddOpDataFailed = 306,
    kInvalidParameter = 307,
    kPidIsNotExist = 308,
    kLeaderIsAlive = 309,
    kNoAliveFollower = 310,
    kPartitionIsAlive = 311,
    kOpStatusIsNotKdoingOrKinited = 312,
    kDropTableError = 313,
    kSetPartitionInfoFailed = 314,
    kConvertColumnDescFailed = 315,
    kCreateTableFailedOnTablet = 316,
    kPidAlreadyExists = 317,
    kSrcEndpointIsNotExistOrNotHealthy = 318,
    kDesEndpointIsNotExistOrNotHealthy = 319,
    kMigrateFailed = 320,
    kNoPidHasUpdate = 321,
    kFailToUpdateTtlFromTablet = 322,
    kFieldNameRepeatedInTableInfo = 323,
    kTheCountOfAddingFieldIsMoreThan63 = 324,
    kFailToUpdateTablemetaForAddingField = 325,
    kConnectZkFailed = 326,
    kRequestTabletFailed = 327,
    kExplainFailed = 328,
    kConvertSchemaFailed = 329,
    kGetSchemaFailed = 330,
    kCheckParameterFailed = 331,
    kCreateProcedureFailedOnTablet = 332,
    kCreateFunctionFailedOnTablet = 333,
    kOPAlreadyExists = 334,
    kOffsetMismatch = 335,
    kReplicaClusterAliasDuplicate = 400,
    kConnectRelicaClusterZkFailed = 401,
    kNotSameReplicaName = 402,
    kConnectNsFailed = 403,
    kReplicaNameNotFound = 404,
    kThisIsNotFollower = 405,
    kTermLeCurTerm = 406,
    kZoneNameNotEqual = 407,
    kAlreadyJoinZone = 408,
    kUnkownServerMode = 409,
    kZoneNotEmpty = 410,
    kCreateZkFailed = 450,
    kGetZkFailed = 451,
    kDelZkFailed = 452,
    kIsFollowerCluster = 453,
    kCurNameserverIsNotLeaderMdoe = 454,
    kShowtableErrorWhenAddReplicaCluster = 455,
    kNoZoneInfo = 501,
    kZoneInfoMismathch = 502,
    kCreateTableForReplicaClusterFailed = 503,
    kAddTaskInReplicaClusterNsFailed = 504,
    kCreateDroptableremoteopForReplicaClusterFailed = 505,
    kNameserverIsNotReplicaCluster = 506,
    kReplicaClusterNotHealthy = 507,
    kReplicaClusterHasNoTableDoNotNeedPid = 508,
    kTableHasNoAliveLeaderPartition = 509,
    kCreateRemoteTableInfoFailed = 510,
    kCreateAddreplicaremoteopFailed = 511,
    kTableHasNoPidXxx = 512,
    kCreateAddreplicassimplyremoteopFailed = 513,
    kRemoteTableHasANoAliveLeaderPartition = 514,
    kRequestHasNoZoneInfoOrTaskInfo = 515,
    kCurNameserverIsLeaderCluster = 516,
    kHasNotColumnKey = 517,
    kTooManyPartition = 518,
    kWrongColumnKey = 519,
    kOperatorNotSupport = 701,
    kDatabaseAlreadyExists = 801,
    kDatabaseNotFound = 802,
    kDatabaseNotEmpty = 803,

    // sql_cmd
    kSQLCmdRunError = 901,

    kSQLCompileError = 1000,
    kSQLRunError = 1001,
    kRPCRunError = 1002,
    kServerConnError = 1003,
    kRPCError = 1004  // brpc controller error
};

struct Status {
    Status(int code_i, std::string msg_i) : code(code_i), msg(msg_i) {}
    Status() : code(ReturnCode::kOk), msg("ok") {}
    inline bool OK() const { return code == ReturnCode::kOk; }
    inline const std::string& GetMsg() const { return msg; }
    inline int GetCode() const { return code; }
    int code;
    std::string msg;
};
}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_STATUS_H_
