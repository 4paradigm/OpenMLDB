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

#include "udf/openmldb_udf.h"

extern "C"
void cut2(::openmldb::base::UDFContext* ctx, ::openmldb::base::StringRef* input, ::openmldb::base::StringRef* output) {
    if (input == nullptr || output == nullptr) {
        return;
    }
    uint32_t size = input->size_ <= 2 ? input->size_ : 2;
    char *buffer = ctx->pool->Alloc(size);
    memcpy(buffer, input->data_, size);
    output->size_ = size;
    output->data_ = buffer;
}

extern "C"
int strlength(::openmldb::base::UDFContext* ctx, ::openmldb::base::StringRef* input) {
    if (input == nullptr) {
        return 0;
    }
    return input->size_;
}

extern "C"
void int2str(::openmldb::base::UDFContext* ctx, int32_t input, ::openmldb::base::StringRef* output) {
    std::string tmp = std::to_string(input);
    char *buffer = ctx->pool->Alloc(tmp.length());
    memcpy(buffer, tmp.data(), tmp.length());
    output->size_ = tmp.length();
    output->data_ = buffer;
}


// udaf example
extern "C"
::openmldb::base::UDFContext* special_sum_init(::openmldb::base::UDFContext* ctx) {
    ctx->ptr = ctx->pool->Alloc(sizeof(int64_t));
    *(reinterpret_cast<int64_t*>(ctx->ptr)) = 10;
    return ctx;
}

extern "C"
::openmldb::base::UDFContext* special_sum_update(::openmldb::base::UDFContext* ctx, int64_t input) {
    int64_t cur = *(reinterpret_cast<int64_t*>(ctx->ptr));
    cur += input;
    *(reinterpret_cast<int*>(ctx->ptr)) = cur;
    return ctx;
}

extern "C"
int64_t special_sum_output(::openmldb::base::UDFContext* ctx) {
    return *(reinterpret_cast<int64_t*>(ctx->ptr)) + 5;
}

// count the number of null value
extern "C"
::openmldb::base::UDFContext* count_null_init(::openmldb::base::UDFContext* ctx) {
    ctx->ptr = ctx->pool->Alloc(sizeof(int64_t));
    *(reinterpret_cast<int64_t*>(ctx->ptr)) = 0;
    return ctx;
}

extern "C"
::openmldb::base::UDFContext* count_null_update(::openmldb::base::UDFContext* ctx,
        ::openmldb::base::StringRef* input, bool is_null) {
    int64_t cur = *(reinterpret_cast<int64_t*>(ctx->ptr));
    if (is_null) {
        cur++;
    }
    *(reinterpret_cast<int*>(ctx->ptr)) = cur;
    return ctx;
}

extern "C"
int64_t count_null_output(::openmldb::base::UDFContext* ctx) {
    return *(reinterpret_cast<int64_t*>(ctx->ptr));
}

// Get the third non-null value of all values
extern "C"
::openmldb::base::UDFContext* third_init(::openmldb::base::UDFContext* ctx) {
    ctx->ptr = reinterpret_cast<void*>(new std::vector<int64_t>());
    return ctx;
}

extern "C"
::openmldb::base::UDFContext* third_update(::openmldb::base::UDFContext* ctx, int64_t input, bool is_null) {
    auto vec = reinterpret_cast<std::vector<int64_t>*>(ctx->ptr);
    if (!is_null && vec->size() < 3) {
        vec->push_back(input);
    }
    return ctx;
}

extern "C"
void third_output(::openmldb::base::UDFContext* ctx, int64_t* output, bool* is_null) {
    auto vec = reinterpret_cast<std::vector<int64_t>*>(ctx->ptr);
    if (vec->size() != 3) {
        *is_null = true;
    } else {
        *is_null = false;
        *output = vec->at(2);
    }
    // free the memory allocated in init function with new/malloc
    delete vec;
}

// Get the first non-null value >= threshold
extern "C"
::openmldb::base::UDFContext* first_ge_init(::openmldb::base::UDFContext* ctx) {
    // threshold init in update
    // threshold, thresh_flag, first_ge, first_ge_flag
    ctx->ptr = reinterpret_cast<void*>(new std::vector<int64_t>(4, 0));
    return ctx;
}

extern "C"
::openmldb::base::UDFContext* first_ge_update(::openmldb::base::UDFContext* ctx, int64_t input, bool is_null, int64_t threshold, bool threshold_is_null) {
    auto pair = reinterpret_cast<std::vector<int64_t>*>(ctx->ptr);
    if (!threshold_is_null && pair->at(1) == 0) {
        pair->at(0) = threshold;
        pair->at(1) = 1;
    }
    if (!is_null && pair->at(3) == 0 && input >= pair->at(0)) {
        pair->at(2) = input;
        pair->at(3) = 1;
    }
    return ctx;
}

extern "C"
void first_ge_output(::openmldb::base::UDFContext* ctx, int64_t* output, bool* is_null) {
    auto pair = reinterpret_cast<std::vector<int64_t>*>(ctx->ptr);
    // threshold is null or no value >= threshold
    if (pair->at(1) == 0 || pair->at(3) == 0) {
        *is_null = true;
    } else {
        *is_null = false;
        *output = pair->at(2);
    }
    // free the memory allocated in init function with new/malloc
    delete pair;
}
