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

#ifndef SRC_CATALOG_KV_ITERATOR_H_
#define SRC_CATALOG_KV_ITERATOR_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <string>

#include "base/slice.h"
#include "proto/tablet.pb.h"

namespace openmldb {
namespace catalog {

class KvIterator {
 public:
    explicit KvIterator(const std::shared_ptr<::google::protobuf::Message>& response) :
        response_(response), buffer_(NULL), is_finish_(true), tsize_(0), offset_(0), c_size_(0), tmp_() {
    }

    virtual ~KvIterator() {}

    uint64_t GetKey() const { return time_; }

    const std::string& GetPK() const { return pk_; }

    openmldb::base::Slice GetValue() const { return tmp_; }

    bool IsFinish() const { return is_finish_; }

    std::shared_ptr<::google::protobuf::Message> GetResponse() const { return response_; }

    virtual void Next() = 0;

    virtual bool Valid() = 0;

 protected:
    std::shared_ptr<::google::protobuf::Message> response_;
    char* buffer_;
    bool is_finish_;
    uint32_t tsize_;
    uint32_t offset_;
    uint32_t c_size_;
    uint64_t time_;
    openmldb::base::Slice tmp_;
    std::string pk_;
};

class ScanKvIterator : public KvIterator {
 public:
    ScanKvIterator(const std::string& pk, const std::shared_ptr<::openmldb::api::ScanResponse>& response)
        : KvIterator(response) {
        buffer_ = reinterpret_cast<char*>(&((*response->mutable_pairs())[0]));
        is_finish_ = response->is_finish();
        tsize_ = response->pairs().size();
        pk_ = pk;
        Next();
    }

    bool Valid() {
        if (offset_ > tsize_) {
            return false;
        }
        if (tsize_ < 12) {
            return false;
        }
        return true;
    }

    void Next() {
        if (offset_ + 4 > tsize_) {
            offset_ += 4;
            return;
        }
        uint32_t block_size = 0;
        memcpy(static_cast<void*>(&block_size), buffer_, 4);
        buffer_ += 4;
        memcpy(static_cast<void*>(&time_), buffer_, 8);
        buffer_ += 8;
        tmp_.reset(buffer_, block_size - 8);
        buffer_ += (block_size - 8);
        offset_ += (4 + block_size);
    }
};

class TraverseKvIterator : public KvIterator {
 public:
    explicit TraverseKvIterator(const std::shared_ptr<::openmldb::api::TraverseResponse>& response)
        : KvIterator(response),
          last_pk_(response->pk()),
          last_ts_(response->ts()) {
        buffer_ = reinterpret_cast<char*>(&((*response->mutable_pairs())[0]));
        is_finish_ = response->is_finish();
        tsize_ = response->pairs().size();
        Next();
    }

    bool Valid() {
        if (offset_ > tsize_) {
            return false;
        }
        if (tsize_ < 16) {
            return false;
        }
        return true;
    }

    void Next() {
        if (offset_ + 8 > tsize_) {
            offset_ += 8;
            return;
        }
        uint32_t total_size = 0;
        memcpy(static_cast<void*>(&total_size), buffer_, 4);
        buffer_ += 4;
        uint32_t pk_size = 0;
        memcpy(static_cast<void*>(&pk_size), buffer_, 4);
        buffer_ += 4;
        memcpy(static_cast<void*>(&time_), buffer_, 8);
        buffer_ += 8;
        pk_.assign(buffer_, pk_size);
        buffer_ += pk_size;
        tmp_.reset(buffer_, total_size - pk_size - 8);
        buffer_ += (total_size - pk_size - 8);
        offset_ += (8 + total_size);
    }

    const std::string& GetLastPK() const { return last_pk_; }

    uint64_t GetLastTS() const { return last_ts_; }

 private:
    std::string last_pk_;
    uint64_t last_ts_;
};

}  // namespace catalog
}  // namespace openmldb
#endif  // SRC_CATALOG_KV_ITERATOR_H_
