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

#ifndef SRC_BASE_KV_ITERATOR_H_
#define SRC_BASE_KV_ITERATOR_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>

#include "base/slice.h"
#include "proto/tablet.pb.h"

namespace openmldb {
namespace base {

class KvIterator {
 public:
    explicit KvIterator(::openmldb::api::ScanResponse* response)
        : response_(response),
          buffer_(NULL),
          tsize_(0),
          offset_(0),
          c_size_(0),
          tmp_(NULL),
          has_pk_(false),
          auto_clean_(true) {
        buffer_ = reinterpret_cast<char*>(&((*response->mutable_pairs())[0]));
        tmp_ = new Slice();
        tsize_ = response->pairs().size();
        Next();
    }

    KvIterator(::openmldb::api::ScanResponse* response, bool clean)
        : response_(response),
          buffer_(NULL),
          tsize_(0),
          offset_(0),
          c_size_(0),
          tmp_(NULL),
          has_pk_(false),
          auto_clean_(clean) {
        buffer_ = reinterpret_cast<char*>(&((*response->mutable_pairs())[0]));
        tsize_ = response->pairs().size();
        tmp_ = new Slice();
        Next();
    }

    ~KvIterator() {
        if (auto_clean_) {
            delete response_;
        }
        delete tmp_;
    }

    bool Valid() {
        if (offset_ > tsize_) {
            return false;
        }
        if (has_pk_) {
            if (tsize_ < 16) {
                return false;
            }
        } else {
            if (tsize_ < 12) {
                return false;
            }
        }
        return true;
    }

    void Next() {
        if (has_pk_) {
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
            tmp_->reset(buffer_, total_size - pk_size - 8);
            buffer_ += (total_size - pk_size - 8);
            offset_ += (8 + total_size);
        } else {
            if (offset_ + 4 > tsize_) {
                offset_ += 4;
                return;
            }
            uint32_t block_size = 0;
            memcpy(static_cast<void*>(&block_size), buffer_, 4);
            buffer_ += 4;
            memcpy(static_cast<void*>(&time_), buffer_, 8);
            buffer_ += 8;
            tmp_->reset(buffer_, block_size - 8);
            buffer_ += (block_size - 8);
            offset_ += (4 + block_size);
        }
    }

    uint64_t GetKey() const { return time_; }

    std::string GetPK() const { return pk_; }

    Slice GetValue() const { return *tmp_; }

 private:
    ::google::protobuf::Message* response_;
    char* buffer_;
    uint32_t tsize_;
    uint32_t offset_;
    uint32_t c_size_;
    uint64_t time_;
    Slice* tmp_;
    std::string pk_;
    bool has_pk_;
    bool auto_clean_;
};

}  // namespace base
}  // namespace openmldb
#endif  // SRC_BASE_KV_ITERATOR_H_
