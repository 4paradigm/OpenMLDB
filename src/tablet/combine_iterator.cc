//
// combine_iterator.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2020-06-04
//
#include "tablet/combine_iterator.h"
#include <algorithm>
#include <utility>
#include "base/glog_wapper.h"

namespace rtidb {
namespace tablet {

CombineIterator::CombineIterator(std::vector<QueryIt> q_its,
                                 uint64_t start_time,
                                 ::rtidb::api::GetType st_type,
                                 const ::rtidb::storage::TTLSt& ttl)
    : q_its_(std::move(q_its)),
      st_(start_time),
      st_type_(st_type),
      ttl_type_(ttl.ttl_type),
      expire_time_(0),
      expire_cnt_(ttl.lat_ttl),
      cur_qit_(nullptr) {
    for (const auto& q_it : q_its_) {
        if (q_it.table) {
            expire_time_ = q_it.table->GetExpireTime(ttl);
            break;
        }
    }
}

void CombineIterator::SeekToFirst() {
    q_its_.erase(std::remove_if(q_its_.begin(), q_its_.end(),
                                [](const QueryIt& q_it) {
                                    return !q_it.table || !q_it.it;
                                }),
                 q_its_.end());
    if (q_its_.empty()) {
        return;
    }
    if (st_type_ == ::rtidb::api::GetType::kSubKeyEq) {
        st_type_ = ::rtidb::api::GetType::kSubKeyLe;
    }
    if (!::rtidb::api::GetType_IsValid(st_type_)) {
        PDLOG(WARNING, "invalid st type %s",
              ::rtidb::api::GetType_Name(st_type_).c_str());
        q_its_.clear();
        return;
    }
    for (auto& q_it : q_its_) {
        if (st_ > 0) {
            if (expire_cnt_ == 0) {
                Seek(q_it.it.get(), st_, st_type_);
            } else {
                switch (ttl_type_) {
                    case ::rtidb::storage::TTLType::kAbsoluteTime:
                        Seek(q_it.it.get(), st_, st_type_);
                        break;
                    case ::rtidb::storage::TTLType::kAbsAndLat:
                        if (!SeekWithCount(q_it.it.get(), st_, st_type_,
                                           expire_cnt_, &q_it.iter_pos)) {
                            Seek(q_it.it.get(), st_, st_type_);
                        }
                        break;
                    default:
                        SeekWithCount(q_it.it.get(), st_, st_type_, expire_cnt_,
                                      &q_it.iter_pos);
                        break;
                }
            }
        } else {
            q_it.it->SeekToFirst();
        }
    }
    SelectIterator();
}

void CombineIterator::SelectIterator() {
    uint64_t max_ts = 0;
    bool need_delete = false;
    cur_qit_ = nullptr;
    for (auto iter = q_its_.begin(); iter != q_its_.end(); iter++) {
        uint64_t cur_ts = 0;
        if (iter->it && iter->it->Valid()) {
            cur_ts = iter->it->GetKey();
            bool is_expire = false;
            switch (ttl_type_) {
                case ::rtidb::storage::TTLType::kAbsoluteTime:
                    if (expire_time_ != 0 && cur_ts <= expire_time_) {
                        is_expire = true;
                    }
                    break;
                case ::rtidb::storage::TTLType::kLatestTime:
                    if (expire_cnt_ != 0 && iter->iter_pos >= expire_cnt_) {
                        is_expire = true;
                    }
                    break;
                case ::rtidb::storage::TTLType::kAbsAndLat:
                    if ((expire_cnt_ != 0 && iter->iter_pos >= expire_cnt_) &&
                        (expire_time_ != 0 && cur_ts <= expire_time_)) {
                        is_expire = true;
                    }
                    break;
                case ::rtidb::storage::TTLType::kAbsOrLat:
                    if ((expire_cnt_ != 0 && iter->iter_pos >= expire_cnt_) ||
                        (expire_time_ != 0 && cur_ts <= expire_time_)) {
                        is_expire = true;
                    }
                    break;
                default:
                    break;
            }
            if (is_expire) {
                iter->it.reset();
                need_delete = true;
                continue;
            }
        }
        if (cur_ts > max_ts) {
            max_ts = cur_ts;
            cur_qit_ = &(*iter);
        }
    }
    if (need_delete) {
        q_its_.erase(
            std::remove_if(q_its_.begin(), q_its_.end(),
                           [](const QueryIt& q_it) { return !q_it.it; }),
            q_its_.end());
    }
}

void CombineIterator::Next() {
    if (cur_qit_ != nullptr) {
        cur_qit_->it->Next();
        cur_qit_->iter_pos += 1;
        cur_qit_ = nullptr;
    }
    SelectIterator();
}

bool CombineIterator::Valid() { return cur_qit_ != nullptr; }

uint64_t CombineIterator::GetTs() { return cur_qit_->it->GetKey(); }

rtidb::base::Slice CombineIterator::GetValue() {
    return cur_qit_->it->GetValue();
}

}  // namespace tablet
}  // namespace rtidb
