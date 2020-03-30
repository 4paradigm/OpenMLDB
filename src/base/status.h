//
// status.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-16
//

#ifndef RTIDB_BASE_STATUS_H
#define RTIDB_BASE_STATUS_H

#include "base/slice.h"

namespace rtidb {
namespace base {

struct ResultMsg {
    int code;
    std::string msg;
};

class Status {
public:
    // Create a success status.
    Status() : state_(NULL) { }
    ~Status() { delete[] state_; }

    // Copy the specified status.
    Status(const Status& s);
    void operator=(const Status& s);

    // Return a success status.
    static Status OK() { return Status(); }

    // Return error status of an appropriate type.
    static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kNotFound, msg, msg2);
    }
    static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kCorruption, msg, msg2);
    }
    static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kNotSupported, msg, msg2);
    }
    static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kInvalidArgument, msg, msg2);
    }
    static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kIOError, msg, msg2);
    }

    static Status InvalidRecord(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kInvalidRecord, msg, msg2);
    }

    static Status WaitRecord() {
        return Status(kWaitRecord, "", "");
    }

    static Status Eof() {
        return Status(kEof, "", ""); 
    }

    // Returns true iff the status indicates success.
    bool ok() const { return (state_ == NULL); }

    // Returns true iff the status indicates a NotFound error.
    bool IsNotFound() const { return code() == kNotFound; }

    // Returns true iff the status indicates a Corruption error.
    bool IsCorruption() const { return code() == kCorruption; }

    // Returns true iff the status indicates an IOError.
    bool IsIOError() const { return code() == kIOError; }

    // Returns true iff the status indicates a NotSupportedError.
    bool IsNotSupportedError() const { return code() == kNotSupported; }

    // Returns true iff the status indicates an InvalidArgument.
    bool IsInvalidArgument() const { return code() == kInvalidArgument; }

    bool IsInvalidRecord() const { return code() == kInvalidRecord; }

    bool IsWaitRecord() const { return code() == kWaitRecord; }
    bool IsEof() const { return code() == kEof; }


    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    std::string ToString() const;

private:
    // OK status has a NULL state_.  Otherwise, state_ is a new[] array
    // of the following form:
    //    state_[0..3] == length of message
    //    state_[4]    == code
    //    state_[5..]  == message
    const char* state_;

    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
        kWaitRecord = 6,
        kEof = 7,
        kInvalidRecord = 8
    };

    Code code() const {
        return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]);
    }

    Status(Code code, const Slice& msg, const Slice& msg2);
    static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) {
    state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
}
inline void Status::operator=(const Status& s) {
    // The following condition catches both aliasing (when this == &s),
    // and the common case where both s and *this are ok.
    if (state_ != s.state_) {
      delete[] state_;
      state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
    }
}

enum ReturnCode {
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
    kFailToCreateFile = 144,
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
    kNameserverIsFollowerAndRequestHasNoZoneInfo = 501,
    kZoneInfoMismathch = 502,
    kCreateCreatetableremoteopForReplicaClusterFailed = 503,
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
    kIndexDeleteFailed = 601,
    kDumpIndexDataFailed = 602,
    kOperatorNotSupport = 701,
};

} // end of base 
} // end of rtidb

#endif /* !RTIDB_BASE_STATUS */
