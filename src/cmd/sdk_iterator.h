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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base/kv_iterator.h"

namespace openmldb {
namespace cmd {

class SDKIterator {
 public:
    SDKIterator(std::vector<std::shared_ptr<::openmldb::base::KvIterator>> iter_vec, uint32_t limit)
        : iter_vec_(iter_vec), cur_iter_(), limit_(limit), cnt_(0) {
        Next();
    }

    bool Valid() {
        if (limit_ > 0) {
            return cur_iter_ && cur_iter_->Valid() && cnt_ <= limit_;
        } else {
            return cur_iter_ && cur_iter_->Valid();
        }
    }

    void Next() {
        if (cur_iter_) {
            cur_iter_->Next();
            cur_iter_.reset();
        }
        uint64_t max_ts = 0;
        bool need_delete = false;
        for (auto& iter : iter_vec_) {
            if (!iter) {
                continue;
            }
            if (!iter->Valid()) {
                need_delete = true;
                iter.reset();
                continue;
            }
            if (iter->GetKey() > max_ts) {
                max_ts = iter->GetKey();
                cur_iter_ = iter;
            }
        }
        if (need_delete) {
            iter_vec_.erase(std::remove_if(iter_vec_.begin(), iter_vec_.end(),
                            [](const std::shared_ptr<::openmldb::base::KvIterator>& it) { return !it; }),
                            iter_vec_.end());
        }
        cnt_++;
    }

    std::string GetPK() { return cur_iter_->GetPK(); }

    uint64_t GetKey() const { return cur_iter_->GetKey(); }

    ::openmldb::base::Slice GetValue() { return cur_iter_->GetValue(); }

 private:
    std::vector<std::shared_ptr<::openmldb::base::KvIterator>> iter_vec_;
    std::shared_ptr<::openmldb::base::KvIterator> cur_iter_;
    uint32_t limit_;
    uint32_t cnt_;
};

}  // namespace cmd
}  // namespace openmldb
