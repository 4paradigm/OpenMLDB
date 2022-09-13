/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef STREAM_SRC_COMMON_COLUMN_ELEMENT_H_
#define STREAM_SRC_COMMON_COLUMN_ELEMENT_H_

#include <string>
#include <memory>
#include <vector>
#include <utility>
#include <boost/algorithm/string.hpp>

#include "common/element.h"

namespace streaming {
namespace interval_join {

class ColumnElement : public Element {
 public:
    ColumnElement(const std::string& key, const std::string& val, int64_t ts, ElementType type = ElementType::kUnknown,
                  const std::string& delimiter = ",");
    ColumnElement(const ColumnElement& ele);
    ColumnElement(ColumnElement&& ele);

    int GetColumnSize() const { return cols_.size(); }

    const std::string* GetColumn(int idx);

    const std::vector<std::string>& cols() const {
        return cols_;
    }

    std::shared_ptr<Element> Clone() const override {
        return std::make_shared<ColumnElement>(*this);
    }

    std::shared_ptr<Element> Clone() override {
        return std::make_shared<ColumnElement>(std::move(*this));
    }

 private:
    std::string delimiter_;
    std::vector<std::string> cols_;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_COMMON_COLUMN_ELEMENT_H_
