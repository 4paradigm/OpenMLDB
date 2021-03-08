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

#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "udf/containers.h"
#include "udf/default_udf_library.h"
#include "udf/udf_registry.h"

using fesql::codec::Date;
using fesql::codec::ListRef;
using fesql::codec::StringRef;
using fesql::codec::Timestamp;
using fesql::codegen::CodeGenContext;
using fesql::codegen::NativeValue;
using fesql::common::kCodegenError;
using fesql::node::TypeNode;

namespace fesql {
namespace udf {

void DefaultUDFLibrary::InitAggByCateUDAFs() {
    InitSumByCateUDAFs();
    InitCountByCateUDAFs();
    InitMinByCateUDAFs();
    InitMaxByCateUDAFs();
    InitAvgByCateUDAFs();
}

}  // namespace udf
}  // namespace fesql
