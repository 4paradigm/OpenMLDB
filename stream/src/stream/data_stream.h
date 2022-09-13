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

#ifndef STREAM_SRC_STREAM_DATA_STREAM_H_
#define STREAM_SRC_STREAM_DATA_STREAM_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/element.h"

namespace streaming {
namespace interval_join {

enum class StreamType { kBase, kProbe, kMix, kUnknown };

enum class FileType {
    kCsv,
    kBinary,
    kParquet
};

class DataStream {
 public:
    explicit DataStream(StreamType type = StreamType::kUnknown) : type_(type) {}
    virtual ~DataStream() {}

    virtual std::shared_ptr<Element> Get() = 0;
    virtual const std::vector<std::shared_ptr<Element>>& elements() const = 0;

    // optional Put API
    virtual int Put(const Element*) { return 0; }
    virtual int Put(Element&&) { return 0; }

    StreamType type() const { return type_; }

    void set_type(StreamType type) { type_ = type; }

    ElementType GetElementType();
    int ReadFromFile(const std::string& path, const std::string& key, const std::string& value,
                     const std::string& ts, bool parse_ts = true, FileType type = FileType::kCsv);
    int WriteToFile(const std::string& path, FileType type = FileType::kCsv);

 private:
    StreamType type_;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_STREAM_DATA_STREAM_H_
