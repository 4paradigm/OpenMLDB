/**
 * Copyright (c) 2023 OpenMLDB Authors
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

#include "vm/sql_ctx.h"

// DONT DELETE: unique_ptr requires full specification for underlying type
#include "zetasql/parser/parser.h"  // IWYU pragma: keep

namespace hybridse {
namespace vm {
SqlContext::SqlContext() {}

SqlContext::~SqlContext() {}

}  // namespace vm
}  // namespace hybridse
