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
#include "vm/mem_catalog.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace vm {


WindowInterface::WindowInterface(bool instance_not_in_window,
                                 int64_t start_offset,
                                 int64_t end_offset,
                                 uint32_t max_size)
    : window_impl_(std::unique_ptr<Window>(
          new CurrentHistoryWindow(true, start_offset, max_size))) {
    window_impl_->set_instance_not_in_window(instance_not_in_window);
}

void WindowInterface::BufferData(uint64_t key, const Row& row) {
    window_impl_->BufferData(key, row);
}

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
        offset += schema_slices.GetSchemaSlice(i)->size();
    }

    auto schema = info->schema_;
    for (int i = 0; i < schema->size(); ++i) {
        if (schema->Get(i).name() == column_expr->GetColumnName()) {
            return offset + i;
        }
    }
    return -1;
}

fesql::codec::Row CoreAPI::RowProject(const RawPtrHandle fn,
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

fesql::codec::Row CoreAPI::WindowProject(const RawPtrHandle fn,
                                         const uint64_t key, const Row row,
                                         const bool is_instance,
                                         WindowInterface* window) {
    return Runner::WindowProject(fn, key, row, is_instance,
                                 window->GetWindow());
}

bool CoreAPI::ComputeCondition(const fesql::vm::RawPtrHandle fn,
                               const Row& row, fesql::codec::RowView* row_view,
                               size_t out_idx) {
    Row cond_row = CoreAPI::RowProject(fn, row, true);
    row_view->Reset(cond_row.buf());
    return Runner::GetColumnBool(row_view, out_idx,
                                 row_view->GetSchema()->Get(out_idx).type());
}

RawPtrHandle CoreAPI::AllocateRaw(size_t bytes) {
    auto buf = malloc(bytes);
    return reinterpret_cast<int8_t*>(buf);
}


void CoreAPI::ReleaseRaw(RawPtrHandle handle) {
    auto buf = const_cast<int8_t*>(reinterpret_cast<const int8_t*>(handle));
    if (buf == nullptr) {
        LOG(ERROR) << "call free to nullptr";
    } else {
        free(buf);
    }
}

void CoreAPI::ReleaseRow(const Row& row) {
    for (int i = 0; i < row.GetRowPtrCnt(); ++i) {
        auto buf = row.buf(i);
        if (buf != nullptr) {
            CoreAPI::ReleaseRaw(buf);
        }
    }
}

}  // namespace vm
}  // namespace fesql
