/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * core_api.cc
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/
#include "vm/core_api.h"
#include "codec/row_codec.h"
namespace fesql {
namespace vm {

fesql::codec::Row CoreAPI::RowProject(const int8_t* fn,
                                      const fesql::codec::Row row,
                                      const bool need_free) {
    if (row.empty()) {
        return fesql::codec::Row();
    }
    int32_t (*udf)(int8_t**, int8_t*, int32_t*, int8_t**) =
        (int32_t(*)(int8_t**, int8_t*, int32_t*, int8_t**))(fn);

    int8_t* buf = nullptr;
    int8_t** row_ptrs = row.GetRowPtrs();
    int32_t* row_sizes = row.GetRowSizes();
    uint32_t ret = udf(row_ptrs, nullptr, row_sizes, &buf);
    if (nullptr != row_ptrs) delete[] row_ptrs;
    if (nullptr != row_sizes) delete[] row_sizes;
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return fesql::codec::Row();
    }
    return Row(reinterpret_cast<char*>(buf),
               fesql::codec::RowView::GetSize(buf), need_free);
}

}  // namespace vm
}