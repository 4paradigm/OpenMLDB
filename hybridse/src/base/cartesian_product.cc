/**
 * Copyright (c) 2024 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/cartesian_product.h"

#include <algorithm>

#include "absl/types/span.h"

namespace hybridse {
namespace base {

int32_t CartesianProductIterSize() { return sizeof(CartesianProductViewIterator); }

static auto cartesian_product(const std::vector<std::vector<int>>& lists) {
    std::vector<std::vector<int>> result;
    if (std::find_if(std::begin(lists), std::end(lists), [](auto e) -> bool { return e.size() == 0; }) !=
        std::end(lists)) {
        return result;
    }
    for (auto& e : lists[0]) {
        result.push_back({e});
    }
    for (size_t i = 1; i < lists.size(); ++i) {
        std::vector<std::vector<int>> temp;
        for (auto& e : result) {
            for (auto f : lists[i]) {
                auto e_tmp = e;
                e_tmp.push_back(f);
                temp.push_back(e_tmp);
            }
        }
        result = temp;
    }
    return result;
}

std::vector<std::vector<int>> cartesian_product(absl::Span<int const> vec) {
    std::vector<std::vector<int>> input;
    for (auto& v : vec) {
        std::vector<int> seq(v, 0);
        for (int i = 0; i < v; ++i) {
            seq[i] = i;
        }
        input.push_back(seq);
    }
    return cartesian_product(input);
}

auto cartesian_product_iterator(absl::Span<int const> vec) { auto products = cartesian_product(vec); }

void CartesianProductIterNew(int32_t* vec, int32_t sz, int8_t* output) {
    auto d = cartesian_product(absl::MakeSpan(vec, sz));
    new (output) CartesianProductViewIterator(d);
}

void CartesianProductIterNext(int8_t* ptr) {
    auto* it = reinterpret_cast<CartesianProductViewIterator*>(ptr);
    if (it != nullptr) {
        it->Next();
    }
}

bool CartesianProductIterValid(int8_t* ptr) {
    auto* it = reinterpret_cast<CartesianProductViewIterator*>(ptr);
    if (it != nullptr) {
        return it->Valid();
    }
    return false;
}

int32_t CartesianProductCount(int8_t* ptr) {
    auto* it = reinterpret_cast<CartesianProductViewIterator*>(ptr);
    if (it != nullptr) {
        return it->data.size();
    }
    return 0;
}

int32_t CartesianProductIterGet(int8_t* ptr, int32_t idx) {
    auto* it = reinterpret_cast<CartesianProductViewIterator*>(ptr);
    if (it != nullptr) {
        return it->GetProduct(idx);
    }
    return 0;
}

void CartesianProductIterDel(int8_t* output) {
    if (output != nullptr) {
        auto* it = reinterpret_cast<CartesianProductViewIterator*>(output);
        if (it != nullptr) {
            it->~CartesianProductViewIterator();
        }
    }
}

}  // namespace base
}  // namespace hybridse
