/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CODEC_FE_ROW_SELECTOR_H_
#define SRC_CODEC_FE_ROW_SELECTOR_H_

#include <utility>
#include <vector>
#include "codec/fe_row_codec.h"
#include "codec/row.h"

namespace fesql {
namespace codec {

class RowSelector {
 public:
    RowSelector(const fesql::codec::Schema* schema,
                const std::vector<size_t>& indices);
    RowSelector(const std::vector<const fesql::codec::Schema*>& schemas,
                const std::vector<std::pair<size_t, size_t>>& indices);

    bool Select(const int8_t* slice, size_t size, int8_t** out_slice,
                size_t* out_size);
    bool Select(const Row& row, int8_t** out_slice, size_t* out_size);

 private:
    fesql::codec::Schema CreateTargetSchema();

    std::vector<const fesql::codec::Schema*> schemas_;
    std::vector<std::pair<size_t, size_t>> indices_;

    fesql::codec::Schema target_schema_;
    std::vector<RowView> row_views_;
    RowBuilder target_row_builder_;
};

}  // namespace codec
}  // namespace fesql
#endif  // SRC_CODEC_FE_ROW_SELECTOR_H_
