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

#ifndef SRC_SDK_FILE_OPTION_PARSER_H_
#define SRC_SDK_FILE_OPTION_PARSER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "base/status.h"
#include "node/node_manager.h"

namespace openmldb {
namespace sdk {

// TODO(zekai): refactor status and error code
class FileOptionsParser {
 public:
    FileOptionsParser() {
        quote_ = '\0';
        check_map_.emplace("format", std::make_pair(CheckFormat(), hybridse::node::kVarchar));
        check_map_.emplace("delimiter", std::make_pair(CheckDelimiter(), hybridse::node::kVarchar));
        check_map_.emplace("null_value", std::make_pair(CheckNullValue(), hybridse::node::kVarchar));
        check_map_.emplace("header", std::make_pair(CheckHeader(), hybridse::node::kBool));
        check_map_.emplace("quote", std::make_pair(CheckQuote(), hybridse::node::kVarchar));
        check_map_.emplace("mode", std::make_pair(CheckMode(), hybridse::node::kVarchar));
    }

    ::openmldb::base::Status Parse(const std::shared_ptr<hybridse::node::OptionsMap>& options_map) {
        for (const auto& item : *options_map) {
            std::string key = item.first;
            boost::to_lower(key);
            auto pair = check_map_.find(key);
            if (pair == check_map_.end()) {
                return {openmldb::base::kSQLCmdRunError, "this option " + key + " is not currently supported"};
            }
            auto status = GetOption(item.second, key, pair->second.first, pair->second.second);
            if (!status.OK()) {
                return status;
            }
        }
        if (delimiter_.find_first_of(quote_) != std::string::npos) {
            return {openmldb::base::kSQLCmdRunError,
                    "delimiter[" + delimiter_ + "] can't include quote[" + quote_ + "]"};
        }
        return {};
    }

    const std::string& GetFormat() const { return format_; }
    const std::string& GetNullValue() const { return null_value_; }
    const std::string& GetDelimiter() const { return delimiter_; }
    bool GetHeader() const { return header_; }
    const char GetQuote() const { return quote_; }
    const std::string& GetMode() const { return mode_; }

 protected:
    std::map<std::string,
             std::pair<std::function<bool(const hybridse::node::ConstNode* node)>, hybridse::node::DataType>>
        check_map_;
    std::string mode_ = "error_if_exists";

 private:
    char quote_;
    // default options
    std::string format_ = "csv";
    std::string null_value_ = "null";
    std::string delimiter_ = ",";
    bool header_ = true;

    ::openmldb::base::Status GetOption(const hybridse::node::ConstNode* node, const std::string& option_name,
                                       std::function<bool(const hybridse::node::ConstNode* node)> const& f,
                                       hybridse::node::DataType option_type) {
        if (node == nullptr) {
            return {base::kSQLCmdRunError, "ERROR: node is nullptr"};
        }
        if (node->GetDataType() != option_type) {
            return {openmldb::base::kSQLCmdRunError, "ERROR: wrong type " +
                hybridse::node::DataTypeName(node->GetDataType()) +
                " for option " + option_name + ", it should be " +
                hybridse::node::DataTypeName(option_type)};
        }
        if (!f(node)) {
            return {base::kSQLCmdRunError, "ERROR: parse option " + option_name + " failed"};
        }
        return {};
    }
    std::function<bool(const hybridse::node::ConstNode* node)> CheckFormat() {
        return [this](const hybridse::node::ConstNode* node) {
            format_ = node->GetAsString();
            if (format_ != "csv") {
                return false;
            }
            return true;
        };
    }
    std::function<bool(const hybridse::node::ConstNode* node)> CheckDelimiter() {
        return [this](const hybridse::node::ConstNode* node) {
            auto tem = node->GetAsString();
            if (tem.size() == 0) {
                return false;
            } else {
                delimiter_ = tem;
                return true;
            }
        };
    }
    std::function<bool(const hybridse::node::ConstNode* node)> CheckNullValue() {
        return [this](const hybridse::node::ConstNode* node) {
            null_value_ = node->GetAsString();
            return true;
        };
    }
    std::function<bool(const hybridse::node::ConstNode* node)> CheckHeader() {
        return [this](const hybridse::node::ConstNode* node) {
            header_ = node->GetBool();
            return true;
        };
    }
    std::function<bool(const hybridse::node::ConstNode* node)> CheckQuote() {
        return [this](const hybridse::node::ConstNode* node) {
            auto tem = node->GetAsString();
            if (tem.size() == 0) {
                quote_ = '\0';
                return true;
            } else if (tem.size() == 1) {
                quote_ = tem[0];
                return true;
            } else {
                return false;
            }
        };
    }
    std::function<bool(const hybridse::node::ConstNode* node)> CheckMode() {
        return [this](const hybridse::node::ConstNode* node) {
            mode_ = node->GetAsString();
            if (mode_ != "error_if_exists" && mode_ != "overwrite" && mode_ != "append") {
                return false;
            }
            return true;
        };
    }
};

class ReadFileOptionsParser : public FileOptionsParser {
 public:
    ReadFileOptionsParser() {
        mode_ = "append";
        check_map_.emplace("load_mode", std::make_pair(CheckLoadMode(), hybridse::node::kVarchar));
        check_map_.emplace("thread", std::make_pair(CheckThread(), hybridse::node::kInt32));
        check_map_.emplace("deep_copy", std::make_pair(CheckDeepCopy(), hybridse::node::kBool));
    }

    const std::string& GetLoadMode() const { return load_mode_; }
    int GetThread() const { return thread_; }
    void SetThread(int thread) { thread_ = thread; }

 private:
    std::string load_mode_ = "cluster";
    int thread_ = 1;
    bool deep_copy_ = true;

    std::function<bool(const hybridse::node::ConstNode* node)> CheckLoadMode() {
        return [this](const hybridse::node::ConstNode* node) {
            load_mode_ = node->GetAsString();
            boost::to_lower(load_mode_);
            if (load_mode_ != "local" && load_mode_ != "cluster") {
                return false;
            }
            return true;
        };
    }

    std::function<bool(const hybridse::node::ConstNode* node)> CheckThread() {
        return [this](const hybridse::node::ConstNode* node) {
            thread_ = node->GetAsInt32();
            if (thread_ <= 0) {
                return false;
            }
            return true;
        };
    }

    std::function<bool(const hybridse::node::ConstNode* node)> CheckDeepCopy() {
        return [this](const hybridse::node::ConstNode* node) {
            deep_copy_ = node->GetBool();
            return true;
        };
    }
};

class WriteFileOptionsParser : public FileOptionsParser {
 public:
    WriteFileOptionsParser() {}
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_FILE_OPTION_PARSER_H_
