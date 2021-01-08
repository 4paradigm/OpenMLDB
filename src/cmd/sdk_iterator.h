
// cmd/sdk_iterator.h
// Copyright (C) 2019 4paradigm.com
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base/kv_iterator.h"

namespace rtidb {
namespace cmd {

class SDKIterator {
 public:
    explicit SDKIterator(
        std::vector<std::shared_ptr<::rtidb::base::KvIterator>> iter_vec,
        uint32_t limit)
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
            iter_vec_.erase(
                std::remove_if(
                    iter_vec_.begin(), iter_vec_.end(),
                    [](const std::shared_ptr<::rtidb::base::KvIterator>& it) {
                        return !it;
                    }),
                iter_vec_.end());
        }
        cnt_++;
    }

    std::string GetPK() { return cur_iter_->GetPK(); }

    uint64_t GetKey() const { return cur_iter_->GetKey(); }

    ::rtidb::base::Slice GetValue() { return cur_iter_->GetValue(); }

 private:
    std::vector<std::shared_ptr<::rtidb::base::KvIterator>> iter_vec_;
    std::shared_ptr<::rtidb::base::KvIterator> cur_iter_;
    uint32_t limit_;
    uint32_t cnt_;
};

}  // namespace cmd
}  // namespace rtidb
