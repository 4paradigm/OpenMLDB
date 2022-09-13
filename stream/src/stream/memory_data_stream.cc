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

#include "stream/memory_data_stream.h"

namespace streaming {
namespace interval_join {

std::shared_ptr<Element> MemoryDataStream::Get() {
    if (elements_.size() == 0) return nullptr;

    int64_t idx = ++idx_;
    if (idx < elements_.size()) {
        return elements_[idx];
    } else {
        --idx_;
        return nullptr;
    }
}

int MemoryDataStream::Put(const Element* element) {
    elements_.emplace_back(element->Clone());
    return 0;
}

int MemoryDataStream::Put(Element&& element) {
    elements_.emplace_back(element.Clone());
    return 0;
}

MergedMemoryDataStream::MergedMemoryDataStream(const std::vector<DataStream*>& streams)
    : MemoryDataStream(StreamType::kMix) {
    std::shared_ptr<Element> eles[streams.size()];
    for (int i = 0; i < streams.size(); i++) {
        eles[i] = nullptr;
    }
    Element* min_ele = nullptr;
    int min_pos = -1;
    do {
        min_ele = nullptr;
        for (int i = 0; i < streams.size(); i++) {
            auto stream = streams[i];
            if (eles[i] == nullptr) {
                eles[i] = stream->Get();
            }

            if (min_ele == nullptr || (eles[i] && eles[i]->ts() < min_ele->ts())) {
                min_ele = eles[i].get();
                min_pos = i;
            }
        }
        if (min_ele) {
            Put(min_ele);
            eles[min_pos] = nullptr;
        }
    } while (min_ele != nullptr);
}

}  // namespace interval_join
}  // namespace streaming
