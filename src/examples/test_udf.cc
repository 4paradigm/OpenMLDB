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
void cut2(UDFContext* ctx, StringRef* input, StringRef* output) {
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
int strlength(UDFContext* ctx, StringRef* input) {
    if (input == nullptr) {
        return 0;
    }
    return input->size_;
}

extern "C"
void int2str(UDFContext* ctx, int32_t input, StringRef* output) {
    std::string tmp = std::to_string(input);
    char *buffer = ctx->pool->Alloc(tmp.length());
    memcpy(buffer, tmp.data(), tmp.length());
    output->size_ = tmp.length();
    output->data_ = buffer;
}


// udaf example
extern "C"
UDFContext* special_sum_init(UDFContext* ctx) {
    ctx->ptr = ctx->pool->Alloc(sizeof(int64_t));
    *(reinterpret_cast<int64_t*>(ctx->ptr)) = 10;
    return ctx;
}

extern "C"
UDFContext* special_sum_update(UDFContext* ctx, int64_t input) {
    int64_t cur = *(reinterpret_cast<int64_t*>(ctx->ptr));
    cur += input;
    *(reinterpret_cast<int*>(ctx->ptr)) = cur;
    return ctx;
}

extern "C"
int64_t special_sum_output(UDFContext* ctx) {
    return *(reinterpret_cast<int64_t*>(ctx->ptr)) + 5;
}
