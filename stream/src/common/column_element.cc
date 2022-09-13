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

#include "common/column_element.h"

namespace streaming {
namespace interval_join {

ColumnElement::ColumnElement(const std::string& key, const std::string& val, int64_t ts, ElementType type,
                             const std::string& delimiter)
    : Element(key, val, ts, type), delimiter_(delimiter) {
    boost::split(cols_, value(), boost::is_any_of(delimiter));
}

ColumnElement::ColumnElement(const ColumnElement& ele)
    : Element(ele), delimiter_(ele.delimiter_), cols_(ele.cols_) {}

ColumnElement::ColumnElement(ColumnElement&& ele)
    : Element(std::move(ele)), delimiter_(std::move(ele.delimiter_)), cols_(std::move(ele.cols_)) {}

const std::string* ColumnElement::GetColumn(int idx) {
    if (idx >= cols_.size()) {
        return nullptr;
    }
    return &cols_.at(idx);
}

}  // namespace interval_join
}  // namespace streaming
