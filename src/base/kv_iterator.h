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

#include <memory>
#include <string>

#include "base/slice.h"
#include "proto/tablet.pb.h"

namespace openmldb {
namespace base {

class KvIterator {
 public:
    explicit KvIterator(const std::shared_ptr<::google::protobuf::Message>& response) :
        response_(response), buffer_(nullptr), is_finish_(true), tsize_(0), offset_(0), tmp_() {}

    virtual ~KvIterator() {}

    uint64_t GetKey() const { return time_; }

    const std::string& GetPK() const { return pk_; }

    Slice GetValue() const { return tmp_; }

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
    uint64_t time_;
    Slice tmp_;
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

    bool Valid();

    void Next();
};

class TraverseKvIterator : public KvIterator {
 public:
    explicit TraverseKvIterator(const std::shared_ptr<::openmldb::api::TraverseResponse>& response)
        : KvIterator(response),
          last_pk_(response->pk()),
          ts_pos_(response->ts_pos()),
          last_ts_(response->ts()) {
        buffer_ = reinterpret_cast<char*>(&((*response->mutable_pairs())[0]));
        is_finish_ = response->is_finish();
        tsize_ = response->pairs().size();
        Next();
    }

    bool Valid();

    void Next();

    void NextPK();

    void Seek(const std::string& pk);

    const std::string& GetLastPK() const { return last_pk_; }

    uint64_t GetLastTS() const { return last_ts_; }

    uint32_t GetTSPos() const { return ts_pos_; }

 private:
    void Reset();

 private:
    std::string last_pk_;
    uint32_t ts_pos_;
    uint64_t last_ts_;
};

}  // namespace base
}  // namespace openmldb
#endif  // SRC_BASE_KV_ITERATOR_H_
