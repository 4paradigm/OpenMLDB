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

#ifndef SRC_LOG_STATUS_H_
#define SRC_LOG_STATUS_H_

#include <string>

#include "base/slice.h"

namespace openmldb {
namespace log {

class Status {
 public:
    // Create a success status.
    Status() : state_(NULL) {}
    ~Status() { delete[] state_; }

    // Copy the specified status.
    Status(const Status& s);
    void operator=(const Status& s);

    // Return a success status.
    static Status OK() { return Status(); }

    // Return error status of an appropriate type.
    static Status NotFound(const ::openmldb::base::Slice& msg,
            const ::openmldb::base::Slice& msg2 = ::openmldb::base::Slice()) {
        return Status(kNotFound, msg, msg2);
    }

    static Status Corruption(const ::openmldb::base::Slice& msg,
            const ::openmldb::base::Slice& msg2 = ::openmldb::base::Slice()) {
        return Status(kCorruption, msg, msg2);
    }

    static Status NotSupported(const ::openmldb::base::Slice& msg,
            const ::openmldb::base::Slice& msg2 = ::openmldb::base::Slice()) {
        return Status(kNotSupported, msg, msg2);
    }

    static Status InvalidArgument(const ::openmldb::base::Slice& msg,
            const ::openmldb::base::Slice& msg2 = ::openmldb::base::Slice()) {
        return Status(kInvalidArgument, msg, msg2);
    }

    static Status IOError(const ::openmldb::base::Slice& msg,
            const ::openmldb::base::Slice& msg2 = ::openmldb::base::Slice()) {
        return Status(kIOError, msg, msg2);
    }

    static Status InvalidRecord(const ::openmldb::base::Slice& msg,
            const ::openmldb::base::Slice& msg2 = ::openmldb::base::Slice()) {
        return Status(kInvalidRecord, msg, msg2);
    }

    static Status WaitRecord() { return Status(kWaitRecord, "", ""); }

    static Status Eof() { return Status(kEof, "", ""); }

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

    Code code() const { return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]); }

    Status(Code code, const ::openmldb::base::Slice& msg, const ::openmldb::base::Slice& msg2);
    static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) { state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_); }
inline void Status::operator=(const Status& s) {
    // The following condition catches both aliasing (when this == &s),
    // and the common case where both s and *this are ok.
    if (state_ != s.state_) {
        delete[] state_;
        state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
    }
}

}  // namespace log
}  // namespace openmldb

#endif  // SRC_LOG_STATUS_H_
