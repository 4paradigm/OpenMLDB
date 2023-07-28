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

#ifndef SRC_SDK_JOB_TABLE_HELPER_H_
#define SRC_SDK_JOB_TABLE_HELPER_H_

#include <memory>
#include <string>
#include <vector>

#include "proto/common.pb.h"
#include "schema/index_util.h"
#include "sdk/result_set.h"

namespace openmldb {
namespace sdk {

using PBOpStatus = ::google::protobuf::RepeatedPtrField<nameserver::OPStatus>;

class JobTableHelper {
 public:
    static bool NeedLikeMatch(const std::string& pattern);
    static bool IsMatch(const std::string& pattern, const std::string& value);

    static std::shared_ptr<hybridse::sdk::ResultSet> MakeResultSet(const PBOpStatus& ops,
            const std::string& like_pattern, ::hybridse::sdk::Status* status);

    static std::shared_ptr<hybridse::sdk::ResultSet> MakeResultSet(
            const std::shared_ptr<hybridse::sdk::ResultSet>& rs,
            const std::string& like_pattern, ::hybridse::sdk::Status* status);

 private:
    static schema::PBSchema GetSchema();
};

}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_JOB_TABLE_HELPER_H_
