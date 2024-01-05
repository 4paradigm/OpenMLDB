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

#ifndef SRC_SDK_INTERACTIVE_H_
#define SRC_SDK_INTERACTIVE_H_

#include <algorithm>
#include <string>

#include "base/status.h"

namespace openmldb {
namespace sdk {

inline const std::string DROP_TABLE_MSG =
    "DROP TABLE is a dangerous operation. Once deleted, it is very difficult to recover. \n"
    "You may also note that: \n"
    "- If a snapshot of a partition is being generated while dropping a table, "
    "the partition will not be deleted successfully.\n"
    "- By default, the deleted data is moved to the folder `recycle`.\n"
    "Please refer to this link for more details: " + base::NOTICE_URL;

inline const std::string DROP_DEPLOYMENT_MSG =
    "- DROP DEPLOYMENT will not delete the index that is created automatically.\n"
    "- DROP DEPLOYMENT will not delete data in the pre-aggregation table in the long window setting.";

inline const std::string DROP_INDEX_MSG =
    "DROP INDEX is a dangerous operation. Once deleted, it is very difficult to recover.\n"
    "You may also note that: \n"
    "- You have to wait for 2 garbage collection intervals (gc_interval) to create the same index.\n"
    "- The index will not be deleted immediately, "
    "it remains until after 2 garbage collection intervals.\n"
    "Please refer to the doc for more details: " + base::NOTICE_URL;

inline const std::string DROP_FUNCTION_MSG =
    "This will lead to execution failure or system crash "
    "if any active deployment is using the function.";

enum class CmdType {
    kDrop = 1,
    kTruncate = 2,
};

enum class TargetType {
    kTable = 1,
    kDeployment = 2,
    kIndex = 3,
    kFunction = 4,
    kProcedure = 5,
};

class InteractiveValidator {
 public:
    InteractiveValidator() = default;
    explicit InteractiveValidator(bool interactive) : interactive_(interactive) {}

    bool Interactive() { return interactive_; }
    void SetInteractive(bool interactive) { interactive_ = interactive; }

    bool Check(CmdType cmd_type, TargetType target, const std::string& name) {
        if (!interactive_) {
            return true;
        }
        std::string msg;
        if (cmd_type == CmdType::kDrop) {
            switch (target) {
                case TargetType::kTable:
                    msg = DROP_TABLE_MSG;
                    break;
                case TargetType::kDeployment:
                    msg = DROP_DEPLOYMENT_MSG;
                    break;
                case TargetType::kIndex:
                    msg = DROP_INDEX_MSG;
                    break;
                case TargetType::kFunction:
                    msg = DROP_FUNCTION_MSG;
                    break;
                default:
                    break;
            }
        }
        if (!msg.empty()) {
            printf("%s\n", msg.c_str());
        }
        std::string cmd_str = CmdType2Str(cmd_type);
        std::string target_str = TargetType2Str(target);
        printf("%s %s %s? yes/no\n", cmd_str.c_str(), target_str.c_str(), name.c_str());
        std::string input;
        std::cin >> input;
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
        if (input != "yes") {
            printf("'%s %s' cmd is canceled!\n", cmd_str.c_str(), name.c_str());
            return false;
        }
        return true;
    }

 private:
    std::string CmdType2Str(CmdType type) {
        if (type == CmdType::kDrop) {
            return "Drop";
        } else {
            return "Truncate";
        }
    }

    std::string TargetType2Str(TargetType type) {
        switch (type) {
            case TargetType::kTable:
                return "table";
            case TargetType::kDeployment:
                return "deployment";
            case TargetType::kIndex:
                return "index";
            case TargetType::kFunction:
                return "function";
            default:
                return "";
        }
        return "";
    }

 private:
    bool interactive_ = false;
};

}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_INTERACTIVE_H_
