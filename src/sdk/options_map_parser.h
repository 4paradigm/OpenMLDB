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

#ifndef SRC_SDK_OPTIONS_MAP_PARSER_H_
#define SRC_SDK_OPTIONS_MAP_PARSER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/ascii.h"
#include "base/status.h"
#include "node/node_manager.h"

namespace openmldb {
namespace sdk {

/* 
hybridse::node::OptionsMap(ptr) to OptionsMapParser::OptionsMap(value), the keys will be lowercase.
Set default in derived class ctor. And write your own validate method by using FindAndPredicate & CheckStatusVec, so
that we can get all options errors in the same time.
*/
class OptionsMapParser {
 public:
    typedef std::map<std::string, hybridse::node::ConstNode> OptionsMap;
    // parse to inner map, validate later
    explicit OptionsMapParser(const std::shared_ptr<hybridse::node::OptionsMap>& options_map) {
        for (const auto& item : *options_map) {
            std::string key = item.first;
            absl::AsciiStrToLower(&key);
            // unordered map, we can't handle the order in options, just ignore
            if (options_map_.find(key) != options_map_.end()) {
                LOG(WARNING) << "option " << key << " already exists, won't replace";
            } else {
                options_map_.insert(std::pair<std::string, hybridse::node::ConstNode>(key, *(item.second)));  // copy
            }
        }
    }
    virtual absl::Status Validate() = 0;
    // You can use absl::StatusOr<std::optional<T>> GetAs() or GetAsxxx() to get value
    const OptionsMap& GetOptionsMap() const { return options_map_; }

    template <typename T>
    absl::StatusOr<T> GetAs(const std::string& key) const {
        auto it = options_map_.find(key);
        if (it == options_map_.end()) {
            return absl::NotFoundError("option not found");
        }
        auto op = it->second.GetAs<T>();
        // option don't have nullopt
        if (!op.ok() || !op.value().has_value()) {
            return absl::InvalidArgumentError("option can't be null, always has value");
        }
        return op.value().value();
    }
    template <typename T>
    void Set(const std::string& key, T value) {
        auto it = options_map_.find(key);
        if (it == options_map_.end()) {
            // impossible
            LOG(WARNING) << "why set a key not found";
        } else {
            options_map_.erase(it);
        }
        options_map_.emplace(std::pair<std::string, hybridse::node::ConstNode>(key, hybridse::node::ConstNode(value)));
    }
    std::string ToString() const {
        std::string str;
        for (const auto& item : options_map_) {
            str.append(item.first).append("=").append(item.second.GetExprString()).append("\n");
        }
        return str;
    }

 protected:
    absl::Status FindAndPredicate(const std::string& key,
                                  std::function<absl::Status(const hybridse::node::ConstNode&)> predicate) {
        auto it = options_map_.find(key);
        if (it == options_map_.end()) {
            return absl::NotFoundError("option not found");
        }
        return predicate(it->second);
    }

    absl::Status CheckStatusVec(const std::vector<absl::Status>& status_vec) {
        // merge all not ok status(actually all invalid arg errs)
        std::string errs;
        for (const auto& status : status_vec) {
            if (!status.ok()) {
                errs.append(status.message()).append("\n");
            }
        }
        return errs.empty() ? absl::OkStatus() : absl::Status(absl::StatusCode::kInvalidArgument, errs);
    }

 protected:
    OptionsMap options_map_;
};

// Validation for local loading, load_mode is special, plz use IsLocalMode() to check it
// DO NOT use this class to validate for cluster loading
class LoadOptionsMapParser : public OptionsMapParser {
 public:
    explicit LoadOptionsMapParser(const std::shared_ptr<hybridse::node::OptionsMap>& options_map)
        : OptionsMapParser(options_map) {
        // emplace: default value if not exists
        options_map_.emplace("delimiter", hybridse::node::ConstNode(","));
        options_map_.emplace("header", hybridse::node::ConstNode(true));
        options_map_.emplace("null_value", hybridse::node::ConstNode("null"));
        options_map_.emplace("format", hybridse::node::ConstNode("csv"));
        options_map_.emplace("quote", hybridse::node::ConstNode("\0"));
        options_map_.emplace("mode", hybridse::node::ConstNode("append"));  // offline is error_is_exists
        options_map_.emplace("deep_copy", hybridse::node::ConstNode(true));
        options_map_.emplace("load_mode", hybridse::node::ConstNode("cluster"));
        options_map_.emplace("thread", hybridse::node::ConstNode(1));
        // writer_type won't used in local loading
    }

    absl::StatusOr<bool> IsLocalMode() {
        auto it = options_map_.find("load_mode");
        if (it == options_map_.end()) {
            return absl::NotFoundError("load_mode not found");
        }
        auto load_mode = it->second.GetAsString();
        absl::AsciiStrToLower(&load_mode);
        if (load_mode == "local") {
            return true;
        }
        if (load_mode == "cluster") {
            return false;
        }
        return absl::InvalidArgumentError("load_mode must be local or cluster");
    }

    absl::Status Validate() override {
        // validation consist with doc
        std::vector<absl::Status> status_vec;
        status_vec.emplace_back(FindAndPredicate("delimiter", [](const hybridse::node::ConstNode& node) {
            return node.GetAsString().empty() ? absl::InvalidArgumentError("delimiter can't be empty")
                                              : absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("header", [](const hybridse::node::ConstNode& node) {
            return node.GetDataType() != hybridse::node::kBool ? absl::InvalidArgumentError("header must be bool")
                                                               : absl::OkStatus();
        }));
        // null_value can be empty
        status_vec.emplace_back(FindAndPredicate("null_value", [](const hybridse::node::ConstNode& node) {
            return node.GetDataType() != hybridse::node::kVarchar
                       ? absl::InvalidArgumentError("null_value must be varchar")
                       : absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("format", [](const hybridse::node::ConstNode& node) {
            auto format = node.GetAsString();
            absl::AsciiStrToLower(&format);
            if (format != "csv") {
                return absl::InvalidArgumentError("local load format must be csv");
            }
            return absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("quote", [](const hybridse::node::ConstNode& node) {
            auto quote = node.GetAsString();
            if (quote.size() > 1) {
                return absl::InvalidArgumentError("quote must be empty or one char");
            }
            return absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("mode", [](const hybridse::node::ConstNode& node) {
            auto mode = node.GetAsString();
            absl::AsciiStrToLower(&mode);
            if (mode != "append") {
                return absl::InvalidArgumentError("local load mode must be append");
            }
            return absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("deep_copy", [](const hybridse::node::ConstNode& node) {
            return node.GetDataType() != hybridse::node::kBool ? absl::InvalidArgumentError("deep_copy must be bool")
                                                               : absl::OkStatus();
        }));
        // to avoid someone forget to use IsLocalMode, still validate here
        // and the validation is for local loading, so load_mode must be local
        status_vec.emplace_back(FindAndPredicate("load_mode", [](const hybridse::node::ConstNode& node) {
            auto load_mode = node.GetAsString();
            absl::AsciiStrToLower(&load_mode);
            if (load_mode != "local") {
                return absl::InvalidArgumentError("load_mode must be local");
            }
            return absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("thread", [](const hybridse::node::ConstNode& node) {
            return node.GetAsInt32() <= 0 ? absl::InvalidArgumentError("thread must be positive") : absl::OkStatus();
        }));
        return CheckStatusVec(status_vec);
    }
};

class WriteOptionsMapParser : public OptionsMapParser {
 public:
    explicit WriteOptionsMapParser(const std::shared_ptr<hybridse::node::OptionsMap>& options_map)
        : OptionsMapParser(options_map) {
        // emplace: default value if not exists
        options_map_.emplace("delimiter", hybridse::node::ConstNode(","));
        options_map_.emplace("header", hybridse::node::ConstNode(true));
        options_map_.emplace("null_value", hybridse::node::ConstNode("null"));
        options_map_.emplace("format", hybridse::node::ConstNode("csv"));
        options_map_.emplace("mode", hybridse::node::ConstNode("error_if_exists"));
        options_map_.emplace("quote", hybridse::node::ConstNode("\0"));
        // coalesce is not supported in local write
    }

    absl::Status Validate() override {
        // validation consist with doc
        std::vector<absl::Status> status_vec;
        status_vec.emplace_back(FindAndPredicate("delimiter", [](const hybridse::node::ConstNode& node) {
            return node.GetAsString().empty() ? absl::InvalidArgumentError("delimiter can't be empty")
                                              : absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("header", [](const hybridse::node::ConstNode& node) {
            return node.GetDataType() != hybridse::node::kBool ? absl::InvalidArgumentError("header must be bool")
                                                               : absl::OkStatus();
        }));
        // null_value can be empty
        status_vec.emplace_back(FindAndPredicate("null_value", [](const hybridse::node::ConstNode& node) {
            return node.GetDataType() != hybridse::node::kVarchar
                       ? absl::InvalidArgumentError("null_value must be string")
                       : absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("format", [](const hybridse::node::ConstNode& node) {
            auto format = node.GetAsString();
            absl::AsciiStrToLower(&format);
            if (format != "csv") {
                return absl::InvalidArgumentError("local load format must be csv");
            }
            return absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("mode", [](const hybridse::node::ConstNode& node) {
            auto mode = node.GetAsString();
            absl::AsciiStrToLower(&mode);
            if (mode != "error_if_exists" && mode != "overwrite" && mode != "append") {
                return absl::InvalidArgumentError("mode must be error_if_exists, overwrite or append");
            }
            return absl::OkStatus();
        }));
        status_vec.emplace_back(FindAndPredicate("quote", [](const hybridse::node::ConstNode& node) {
            auto quote = node.GetAsString();
            if (quote.size() > 1) {
                return absl::InvalidArgumentError("quote must be empty or one char");
            }
            return absl::OkStatus();
        }));
        return CheckStatusVec(status_vec);
    }
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_OPTIONS_MAP_PARSER_H_
