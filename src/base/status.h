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
    OK = 0,
    TABLE_IS_NOT_EXIST = 100,
    TABLE_ALREADY_EXISTS = 101,
    TABLE_IS_LEADER = 102,
    TABLE_IS_FOLLOWER = 103,
    TABLE_IS_LOADING = 104,
    TABLE_STATUS_IS_NOT_KNORMAL = 105,
    TABLE_STATUS_IS_KMAKINGSNAPSHOT = 106,
    TABLE_STATUS_IS_NOT_KSNAPSHOTPAUSED = 107,
    IDX_NAME_NOT_FOUND = 108,
    KEY_NOT_FOUND = 109,
    REPLICATOR_IS_NOT_EXIST = 110,
    SNAPSHOT_IS_NOT_EXIST = 111,
    TTL_TYPE_MISMATCH = 112,
    TS_MUST_BE_GREATER_THAN_ZERO = 114,
    INVALID_DIMENSION_PARAMETER = 115,
    PUT_FAILED = 116,
    ST_LESS_THAN_ET = 117,
    REACHE_THE_SCAN_MAX_BYTES_SIZE = 118,
    REPLICA_ENDPOINT_ALREADY_EXISTS = 119,
    FAIL_TO_ADD_REPLICA_ENDPOINT = 120,
    REPLICATOR_ROLE_IS_NOT_LEADER = 121,
    FAIL_TO_APPEND_ENTRIES_TO_REPLICATOR = 122,
    FILE_RECEIVER_INIT_FAILED = 123,
    CANNOT_FIND_RECEIVER = 124,
    BLOCK_ID_MISMATCH = 125,
    RECEIVE_DATA_ERROR = 126,
    WRITE_DATA_FAILED = 127,
    SNAPSHOT_IS_SENDING = 128,
    TABLE_META_IS_ILLEGAL = 129,
    TABLE_DB_PATH_IS_NOT_EXIST = 130,
    CREATE_TABLE_FAILED = 131,
    TTL_IS_GREATER_THAN_CONF_VALUE = 132,
    CANNOT_UPDATE_TTL_BETWEEN_ZERO_AND_NONZERO = 133,
    NO_FOLLOWER = 134,
    INVALID_CONCURRENCY = 135,
    DELETE_FAILED = 136,
    TS_NAME_NOT_FOUND = 137,
    FAIL_TO_GET_DB_ROOT_PATH = 138,
    FAIL_TO_GET_RECYCLE_ROOT_PATH = 139,
    NAMESERVER_IS_NOT_LEADER = 300,
    AUTO_FAILOVER_IS_ENABLED = 301,
    ENDPOINT_IS_NOT_EXIST = 302,
    TABLET_IS_NOT_HEALTHY = 303,
    SET_ZK_FAILED = 304,
    CREATE_OP_FAILED = 305,
    ADD_OP_DATA_FAILED = 306,
    INVALID_PARAMETER = 307,
    PID_IS_NOT_EXIST = 308,
    LEADER_IS_ALIVE = 309,
    NO_ALIVE_FOLLOWER = 310,
    PARTITION_IS_ALIVE = 311,
    OP_STATUS_IS_NOT_KDOING_OR_KINITED = 312,
    DROP_TABLE_ERROR = 313,
    SET_PARTITION_INFO_FAILED = 314,
    CONVERT_COLUMN_DESC_FAILED = 315,
    CREATE_TABLE_FAILED_ON_TABLET = 316,
    PID_ALREADY_EXISTS = 317,
    SRC_ENDPOINT_IS_NOT_EXIST_OR_NOT_HEALTHY = 318,
    DES_ENDPOINT_IS_NOT_EXIST_OR_NOT_HEALTHY = 319,
    MIGRATE_FAILED = 320,
    NO_PID_HAS_UPDATE = 321,
    FAIL_TO_UPDATE_TTL_FROM_TABLET = 322,
    FIELD_NAME_REPEATED_IN_TABLE_INFO = 323,
    THE_COUNT_OF_ADDING_FIELD_IS_MORE_THAN_63 = 324,
    FAIL_TO_UPDATE_TABLEMETA_FOR_ADDING_FIELD = 325,
    CONNECT_ZK_FAILED = 326,
    REPLICA_CLUSTER_ALIAS_DUPLICATE = 400,
    CONNECT_RELICA_CLUSTER_ZK_FAILED = 401,
    NOT_SAME_REPLICA_NAME = 402,
    CONNECT_NS_FAILED = 403,
    REPLICA_NAME_NOT_FOUND = 404,
    THIS_IS_NOT_FOLLOWER = 405,
    TERM_LE_CUR_TERM = 406,
    ZONE_NAME_NOT_EQUAL = 407,
    ALREADY_JOIN_ZONE = 408,
    UNKOWN_SERVER_MODE = 409,
    ZONE_NOT_EMPTY = 410,
    CREATE_ZK_FAILED = 450,
    GET_ZK_FAILED = 451,
    DEL_ZK_FAILED = 452,
    IS_FOLLOWER_CLUSTER = 453,
    CUR_NAMESERVER_IS_NOT_LEADER_MDOE = 454,
    SHOWTABLE_ERROR_WHEN_ADD_REPLICA_CLUSTER = 455,
    NAMESERVER_IS_FOLLOWER_AND_REQUEST_HAS_NO_ZONE_INFO = 501,
    ZONE_INFO_MISMATHCH = 502,
    CREATE_CREATETABLEREMOTEOP_FOR_REPLICA_CLUSTER_FAILED = 503,
    ADD_TASK_IN_REPLICA_CLUSTER_NS_FAILED = 504,
    CREATE_DROPTABLEREMOTEOP_FOR_REPLICA_CLUSTER_FAILED = 505,
    NAMESERVER_IS_NOT_REPLICA_CLUSTER = 506,
    REPLICA_CLUSTER_NOT_HEALTHY = 507,
    REPLICA_CLUSTER_HAS_NO_TABLE_DO_NOT_NEED_PID = 508,
    TABLE_HAS_A_NO_ALIVE_LEADER_PARTITION = 509,
    CREATE_REMOTE_TABLE_INFO_FAILED = 510,
    CREATE_ADDREPLICAREMOTEOP_FAILED = 511,
    TABLE_HAS_NO_PID_XXX = 512,
    CREATE_ADDREPLICASSIMPLYREMOTEOP_FAILED = 513,
    REMOTE_TABLE_HAS_A_NO_ALIVE_LEADER_PARTITION = 514,
    REQUEST_HAS_NO_ZONE_INFO_OR_TASK_INFO = 515,
    CUR_NAMESERVER_IS_LEADER_CLUSTER = 516,
    INDEX_DELETE_FAILED = 601,
    OPERATOR_NOT_SUPPORT = 701,
};

} // end of base 
} // end of rtidb

#endif /* !RTIDB_BASE_STATUS */
