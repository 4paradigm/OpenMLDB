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

#include "stream/simple_data_stream.h"

#include <absl/strings/str_cat.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <memory>

#include "common/column_element.h"

namespace streaming {
namespace interval_join {

DEFINE_bool(gen_ts_unique, true, "whether generate ts uniquely (no same ts across keys)");

SimpleDataStream::SimpleDataStream(int64_t count, int64_t key_num, int64_t max_ts, StreamType type)
    : count_(count), key_num_(key_num), idx_(0), DataStream(type) {
    if (type == StreamType::kMix) {
        LOG(ERROR) << "Unsupport Mix StreamType for SimpleDataStream";
    }
    if (max_ts < count - 1) {
        max_ts = count - 1;
    }
    max_ts_ = max_ts;

    elements_.resize(count);
    int64_t step = max_ts_ / count;
    if (step < 1) {
        step = 1;
    }
    int64_t count_per_key = count / key_num;
    if (count_per_key * key_num != count) {
        LOG(ERROR) << "element count is not divided by key_num!";
        return;
    }

    for (int64_t i = 0; i < count_per_key; i++) {
        for (int k = 0; k < key_num; k++) {
            int64_t idx = i * key_num + k;
            int64_t ts = 0;
            if (FLAGS_gen_ts_unique) {
                ts = (i * key_num + k) * step;
            } else {
                ts = i * step;
            }
            elements_[idx] = std::make_shared<ColumnElement>(absl::StrCat("key_", std::to_string(k)),
                                                           std::to_string(ts), ts);
            switch (type) {
                case StreamType::kBase:
                    elements_[idx]->set_type(ElementType::kBase);
                    break;
                case StreamType::kProbe:
                    elements_[idx]->set_type(ElementType::kProbe);
                    break;
                default:
                    elements_[idx]->set_type(ElementType::kUnknown);
                    break;
            }
        }
    }
    LOG(WARNING) << "Pre-load " << count << " elements in the stream";
}

std::shared_ptr<Element> SimpleDataStream::Get() {
    auto idx = idx_.fetch_add(1);
    if (idx < count_) {
        return elements_.at(idx);
    } else {
        return nullptr;
    }
}

}  // namespace interval_join
}  // namespace streaming
