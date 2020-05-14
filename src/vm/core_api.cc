/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * core_api.cc
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/
#include "vm/core_api.h"
#include "codec/fe_row_codec.h"
#include "vm/runner.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace vm {

int CoreAPI::ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                fesql::node::ColumnRefNode* expr) {
    auto& schema_slices = node->GetOutputNameSchemaList();
    SchemasContext schema_ctx(schema_slices);
    const RowSchemaInfo* info;
    auto column_expr = dynamic_cast<const node::ColumnRefNode*>(expr);
    if (!schema_ctx.ColumnRefResolved(column_expr->GetRelationName(),
            column_expr->GetColumnName(), &info)) {
        LOG(WARNING) << "Resolve column expression failed";
        return -1;
    }

    int offset = 0;
    for (int i = 0; i < info->idx_; ++i) {
        offset += schema_slices[i].second->size();
    }

    auto schema = info->schema_;
    for (int i = 0; i < schema->size(); ++i) {
        if (schema->Get(i).name() == column_expr->GetColumnName()) {
            return offset + i;
        }
    }
    return -1;
}

fesql::codec::Row CoreAPI::RowProject(const RawFunctionPtr fn,
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

fesql::codec::Row CoreAPI::WindowProject(const RawFunctionPtr fn,
                                         const uint64_t key, const Row row,
                                         const bool is_instance,
                                         WindowInterface* window) {
    return Runner::WindowProject(fn, key, row, is_instance,
                                 window->GetWindow());
}

bool CoreAPI::ComputeCondition(const fesql::vm::RawFunctionPtr fn,
                               const Row& row, fesql::codec::RowView* row_view,
                               size_t out_idx) {
    Row cond_row = CoreAPI::RowProject(fn, row, true);
    row_view->Reset(cond_row.buf());
    return Runner::GetColumnBool(row_view, out_idx,
                                 row_view->GetSchema()->Get(out_idx).type());
}

}  // namespace vm
}  // namespace fesql
