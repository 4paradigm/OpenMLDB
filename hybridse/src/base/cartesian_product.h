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

#ifndef HYBRIDSE_SRC_BASE_CARTESIAN_PRODUCT_H_
#define HYBRIDSE_SRC_BASE_CARTESIAN_PRODUCT_H_

#include <cstdint>
#include <vector>

#include "absl/types/span.h"

namespace hybridse {
namespace base {

using CartesianProductViewForIndex = std::vector<std::vector<int>>;

struct CartesianProductViewIterator {
    explicit CartesianProductViewIterator(const CartesianProductViewForIndex& d) : data(d) { it = data.cbegin(); }

    CartesianProductViewForIndex ::const_iterator Next() { return std::next(it); }
    bool Valid() const { return it != data.cend(); }

    int GetProduct(int i) const { return it->at(i); }

    CartesianProductViewForIndex data;
    CartesianProductViewForIndex ::const_iterator it;
};

std::vector<std::vector<int>> cartesian_product(absl::Span<int const> vec);

int32_t CartesianProductIterSize();
void CartesianProductIterNew(int32_t* vec, int32_t sz, int8_t* ptr);
int32_t CartesianProductCount(int8_t* ptr);
int32_t CartesianProductIterGet(int8_t* ptr, int32_t idx);
void CartesianProductIterNext(int8_t* ptr);
bool CartesianProductIterValid(int8_t* ptr);
void CartesianProductIterDel(int8_t* ptr);

}  // namespace base
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_BASE_CARTESIAN_PRODUCT_H_
