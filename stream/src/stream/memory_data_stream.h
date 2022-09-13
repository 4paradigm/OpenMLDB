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

#ifndef STREAM_SRC_STREAM_MEMORY_DATA_STREAM_H_
#define STREAM_SRC_STREAM_MEMORY_DATA_STREAM_H_

#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/element.h"
#include "stream/data_stream.h"

namespace streaming {
namespace interval_join {

/*
 * do NOT support interleaving `Put()` and `Get()`
 * Put is not thread-safe
 * 1. Put() all elements to the stream
 * 2. Get() in multiple threads
 */
class MemoryDataStream : public DataStream {
 public:
    explicit MemoryDataStream(StreamType type = StreamType::kUnknown) : DataStream(type) {}

    std::shared_ptr<Element> Get() override;
    int Put(const Element* element) override;
    int Put(Element&& element) override;
    const std::vector<std::shared_ptr<Element>>& elements() const override { return elements_; }

    virtual size_t size() const { return elements_.size() - idx_ - 1; }

 protected:
    std::vector<std::shared_ptr<Element>> elements_;
    std::atomic<int64_t> idx_ = -1;
};

class MergedMemoryDataStream : public MemoryDataStream {
 public:
    // if ts equals, stream before will have priority
    explicit MergedMemoryDataStream(const std::vector<DataStream*>& streams);
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_STREAM_MEMORY_DATA_STREAM_H_
