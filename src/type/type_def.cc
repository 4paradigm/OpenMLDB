/*
 * type_def.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "type/type_def.h"
#include "absl/time/clock.h"

namespace fesql {
namespace type {

TableBuilder::TableBuilder(uint64_t tid, uint64_t cid,
        const std::string& name):table_(), offset_(4) {
    table_.table_id = tid;
    table_.catalog_id = cid;
    table_.name = name;
    table_.ctime = ::absl::Now();
}

void TableBuilder::AddColumn(const TypeDef& type, 
        const std::string& name) {
    ColumnDef cdef;
    cdef.name = name;
    cdef.type = type;
    cdef.offset = offset_;
    table_.columns.push_back(cdef);
    offset_ += 2;
}

Table& TableBuilder::Build() {
    return table_;
}

} // namespace of type
} // namespace of fesql
