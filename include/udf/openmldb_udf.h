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

#ifndef INCLUDE_UDF_OPENMLDB_UDF_H_
#define INCLUDE_UDF_OPENMLDB_UDF_H_

#include "base/mem_pool.h"
#include "base/string_ref.h"
#include "base/type.h"
namespace openmldb {
namespace base {
constexpr int OPENMLDB_UDF_LIBRARY_VERSION = 1;

struct UDFContext {
    ::openmldb::base::ByteMemoryPool* pool;
    void* ptr;
};
}  // namespace base
}  // namespace openmldb
#endif  // INCLUDE_UDF_OPENMLDB_UDF_H_
