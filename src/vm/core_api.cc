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
#include "udf/udf.h"
#include "vm/mem_catalog.h"
#include "vm/runner.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace vm {

WindowInterface::WindowInterface(bool instance_not_in_window,
                                 int64_t start_offset, int64_t end_offset,
                                 uint64_t row_preceding, uint32_t max_size)
    : window_impl_(std::unique_ptr<Window>(
          new CurrentHistoryWindow(start_offset, max_size))) {
    window_impl_->set_rows_preceding(row_preceding);
    window_impl_->set_instance_not_in_window(instance_not_in_window);
}

void WindowInterface::BufferData(uint64_t key, const Row& row) {
    window_impl_->BufferData(key, row);
}
int CoreAPI::ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                int32_t schema_idx, int32_t column_idx) {
    SchemasContext schema_ctx(node->GetOutputNameSchemaList());
    return schema_ctx.ColumnOffsetResolved(schema_idx, column_idx);
}
int CoreAPI::ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                fesql::node::ColumnRefNode* expr) {
    SchemasContext schema_ctx(node->GetOutputNameSchemaList());
    auto column_expr = dynamic_cast<const node::ColumnRefNode*>(expr);
    return schema_ctx.ColumnOffsetResolved(column_expr->GetRelationName(),
                                           column_expr->GetColumnName());
}

fesql::codec::Row CoreAPI::RowConstProject(const RawPtrHandle fn,
                                           const bool need_free) {
    auto udf =
        reinterpret_cast<int32_t (*)(const int8_t*, const int8_t*, int8_t**)>(
            const_cast<int8_t*>(fn));

    int8_t* buf = nullptr;
    uint32_t ret = udf(nullptr, nullptr, &buf);
    fesql::udf::ThreadLocalMemoryPoolReset();
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return fesql::codec::Row();
    }
    return Row(base::RefCountedSlice::CreateManaged(
        buf, fesql::codec::RowView::GetSize(buf)));
}
fesql::codec::Row CoreAPI::RowProject(const RawPtrHandle fn,
                                      const fesql::codec::Row row,
                                      const bool need_free) {
    if (row.empty()) {
        return fesql::codec::Row();
    }
    auto udf =
        reinterpret_cast<int32_t (*)(const int8_t*, const int8_t*, int8_t**)>(
            const_cast<int8_t*>(fn));

    auto row_ptr = reinterpret_cast<const int8_t*>(&row);
    int8_t* buf = nullptr;
    uint32_t ret = udf(row_ptr, nullptr, &buf);
    fesql::udf::ThreadLocalMemoryPoolReset();
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return fesql::codec::Row();
    }
    return Row(base::RefCountedSlice::CreateManaged(
        buf, fesql::codec::RowView::GetSize(buf)));
}

fesql::codec::Row CoreAPI::WindowProject(const RawPtrHandle fn,
                                         const uint64_t key, const Row row,
                                         const bool is_instance,
                                         size_t append_slices,
                                         WindowInterface* window) {
    return Runner::WindowProject(fn, key, row, is_instance, append_slices,
                                 window->GetWindow());
}

fesql::codec::Row CoreAPI::GroupbyProject(const RawPtrHandle fn,
                                          fesql::vm::MemTableHandler* table) {
    return Runner::GroupbyProject(fn, table);
}

bool CoreAPI::ComputeCondition(const fesql::vm::RawPtrHandle fn, const Row& row,
                               fesql::codec::RowView* row_view,
                               size_t out_idx) {
    Row cond_row = CoreAPI::RowProject(fn, row, true);
    row_view->Reset(cond_row.buf());
    return Runner::GetColumnBool(row_view, out_idx,
                                 row_view->GetSchema()->Get(out_idx).type());
}

fesql::codec::Row* CoreAPI::NewRow(size_t bytes) {
    auto buf = reinterpret_cast<int8_t*>(malloc(bytes));
    if (buf == nullptr) {
        return nullptr;
    }
    auto slice = base::RefCountedSlice::CreateManaged(buf, bytes);
    return new fesql::codec::Row(slice);
}

RawPtrHandle CoreAPI::GetRowBuf(fesql::codec::Row* row, size_t idx) {
    return row->buf(idx);
}

RawPtrHandle CoreAPI::AppendRow(fesql::codec::Row* row, size_t bytes) {
    auto buf = reinterpret_cast<int8_t*>(malloc(bytes));
    if (buf == nullptr) {
        return nullptr;
    }
    auto slice = base::RefCountedSlice::CreateManaged(buf, bytes);
    row->Append(slice);
    return buf;
}

fesql::vm::MemTableHandler* CoreAPI::NewMemTableHandler(
    const std::string& table_name, const std::string& db,
    const fesql::codec::Schema& schema) {
    return new vm::MemTableHandler(table_name, db, &schema);
}

void CoreAPI::AddRowToMemTable(fesql::vm::MemTableHandler* table_handler,
                               fesql::codec::Row* row) {
    table_handler->AddRow(*row);
}

}  // namespace vm
}  // namespace fesql
