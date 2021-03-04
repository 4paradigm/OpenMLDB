/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * agg_by_category_def.cc
 *--------------------------------------------------------------------------
 **/
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
