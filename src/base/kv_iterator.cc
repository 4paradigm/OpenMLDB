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

#include "base/kv_iterator.h"

namespace openmldb {
namespace base {

bool ScanKvIterator::Valid() {
    if (offset_ > tsize_) {
        return false;
    }
    if (tsize_ < 12) {
        return false;
    }
    return true;
}

void ScanKvIterator::Next() {
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

bool TraverseKvIterator::Valid() {
    if (offset_ > tsize_) {
        return false;
    }
    if (tsize_ < 16) {
        return false;
    }
    return true;
}

void TraverseKvIterator::Next() {
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

void TraverseKvIterator::NextPK() {
    std::string cur_pk = pk_;
    do {
        Next();
    } while (Valid() && pk_ == cur_pk);
}

void TraverseKvIterator::Reset() {
    auto response = std::dynamic_pointer_cast<::openmldb::api::TraverseResponse>(response_);
    buffer_ = reinterpret_cast<char*>(&((*response->mutable_pairs())[0]));
    offset_ = 0;
}

void TraverseKvIterator::Seek(const std::string& pk) {
    Reset();
    do {
        Next();
    } while (Valid() && pk_ != pk);
}

}  // namespace base
}  // namespace openmldb
