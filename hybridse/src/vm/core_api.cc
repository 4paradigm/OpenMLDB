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

#include "vm/core_api.h"
#include "base/sig_trace.h"
#include "codec/fe_row_codec.h"
#include "vm/jit_runtime.h"
#include "vm/mem_catalog.h"
#include "vm/runner.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace vm {

WindowInterface::WindowInterface(bool instance_not_in_window, bool exclude_current_time, bool exclude_current_row,
                                 const std::string& frame_type_str, int64_t start_offset, int64_t end_offset,
                                 uint64_t rows_preceding, uint64_t max_size, bool without_order_by) {
    if (exclude_current_row && max_size > 0 && end_offset == 0) {
        max_size++;
    }
    window_impl_ = std::make_unique<HistoryWindow>(
        WindowRange(ExtractFrameType(frame_type_str), start_offset, end_offset, rows_preceding, max_size));
    window_impl_->set_instance_not_in_window(instance_not_in_window);
    window_impl_->set_exclude_current_time(exclude_current_time);
    window_impl_->set_without_order_by(without_order_by);
}

bool WindowInterface::BufferData(uint64_t key, const Row& row) {
    return window_impl_->BufferData(key, row);
}

Window::WindowFrameType WindowInterface::ExtractFrameType(
    const std::string& frame_type_str) const {
    if (frame_type_str == "kFrameRows") {
        return Window::kFrameRows;
    } else if (frame_type_str == "kFrameRowsRange") {
        return Window::kFrameRowsRange;
    } else if (frame_type_str == "kFrameRowsMergeRowsRange") {
        return Window::kFrameRowsMergeRowsRange;
    } else {
        LOG(WARNING) << "Illegal frame type: " << frame_type_str;
        return Window::kFrameRows;
    }
}

int CoreAPI::ResolveColumnIndex(hybridse::vm::PhysicalOpNode* node,
                                hybridse::node::ExprNode* expr) {
    const SchemasContext* schemas_ctx = node->schemas_ctx();
    size_t schema_idx;
    size_t col_idx;
    Status status;
    switch (expr->GetExprType()) {
        case node::kExprColumnRef: {
            auto column_ref = dynamic_cast<const node::ColumnRefNode*>(expr);
            status = schemas_ctx->ResolveColumnRefIndex(column_ref, &schema_idx,
                                                        &col_idx);
            break;
        }
        case node::kExprColumnId: {
            auto column_id = dynamic_cast<const node::ColumnIdNode*>(expr);
            status = schemas_ctx->ResolveColumnIndexByID(
                column_id->GetColumnID(), &schema_idx, &col_idx);
            break;
        }
        default:
            return -1;
    }

    if (!status.isOK()) {
        LOG(WARNING) << "Fail to resolve column " << expr->GetExprString();
        return -1;
    }
    size_t total_offset = col_idx;
    for (size_t i = 0; i < schema_idx; ++i) {
        total_offset += node->GetOutputSchemaSource(i)->size();
    }
    return total_offset;
}

std::string CoreAPI::ResolveSourceColumnName(
    hybridse::vm::PhysicalOpNode* node, hybridse::node::ColumnRefNode* expr) {
    const SchemasContext* schemas_ctx = node->schemas_ctx();
    auto column_expr = dynamic_cast<const node::ColumnRefNode*>(expr);
    size_t column_id;
    int child_path_idx;
    size_t child_column_id;
    size_t source_column_id;
    const PhysicalOpNode* source_node = nullptr;
    auto status = schemas_ctx->ResolveColumnID(
        column_expr->GetDBName(),
        column_expr->GetRelationName(), column_expr->GetColumnName(),
        &column_id, &child_path_idx, &child_column_id, &source_column_id,
        &source_node);
    if (!status.isOK() || source_node == nullptr) {
        LOG(WARNING) << "Fail to resolve column "
                     << column_expr->GetExprString();
        return "";
    }
    size_t schema_idx;
    size_t col_idx;
    status = source_node->schemas_ctx()->ResolveColumnIndexByID(
        source_column_id, &schema_idx, &col_idx);
    return source_node->schemas_ctx()
        ->GetSchemaSource(schema_idx)
        ->GetColumnName(col_idx);
}

ColumnSourceInfo CoreAPI::ResolveSourceColumn(
    hybridse::vm::PhysicalOpNode* node,
    const std::string& db_name,
    const std::string& relation_name,
    const std::string& column_name) {
    ColumnSourceInfo result;
    if (node == nullptr) {
        return result;
    }
    auto& status = result.status_;
    status = node->schemas_ctx()->ResolveColumnID(db_name,
        relation_name, column_name, &result.column_id_, &result.child_path_idx_,
        &result.child_column_id_, &result.source_column_id_,
        &result.source_node_);
    if (!status.isOK() || result.source_node_ == nullptr) {
        return result;
    }

    size_t schema_idx;
    size_t col_idx;
    status = node->schemas_ctx()->ResolveColumnIndexByID(result.column_id_,
                                                         &schema_idx, &col_idx);
    if (!status.isOK()) {
        return result;
    }
    result.schema_idx_ = static_cast<int>(schema_idx);
    result.col_idx_ = static_cast<int>(col_idx);
    result.total_col_idx_ = col_idx;
    for (size_t i = 0; i < schema_idx; ++i) {
        result.total_col_idx_ += node->GetOutputSchemaSource(i)->size();
    }

    size_t source_schema_idx;
    size_t source_col_idx;
    auto source_schemas_ctx = result.source_node_->schemas_ctx();
    status = source_schemas_ctx->ResolveColumnIndexByID(
        result.source_column_id_, &source_schema_idx, &source_col_idx);
    if (!status.isOK()) {
        return result;
    }
    result.source_schema_idx_ = static_cast<int>(source_schema_idx);
    result.source_col_idx_ = static_cast<int>(source_col_idx);
    result.source_total_col_idx_ = source_col_idx;
    for (size_t i = 0; i < source_schema_idx; ++i) {
        result.source_total_col_idx_ +=
            source_schemas_ctx->GetSchemaSource(i)->size();
    }
    result.source_col_name_ =
        source_schemas_ctx->GetSchemaSource(source_schema_idx)
            ->GetColumnName(source_col_idx);
    return result;
}

size_t CoreAPI::GetUniqueID(const hybridse::vm::PhysicalOpNode* node) {
    return node->node_id();
}

GroupbyInterface::GroupbyInterface(const hybridse::codec::Schema& schema)
    : mem_table_handler_(new vm::MemTableHandler(&schema)) {}

void GroupbyInterface::AddRow(hybridse::codec::Row* row) {
    mem_table_handler_->AddRow(*row);
}

hybridse::vm::TableHandler* GroupbyInterface::GetTableHandler() {
    return mem_table_handler_;
}

hybridse::codec::Row CoreAPI::RowConstProject(const RawPtrHandle fn,
                                              const Row parameter,
                                              const bool need_free) {
    // Init current run step runtime
    JitRuntime::get()->InitRunStep();

    auto udf = reinterpret_cast<int32_t (*)(const int64_t, const int8_t*,
                                            const int8_t*, const int8_t*, int8_t**)>(
        const_cast<int8_t*>(fn));
    int8_t* buf = nullptr;
    auto parameter_ptr = reinterpret_cast<const int8_t*>(&parameter);
    uint32_t ret = udf(0, nullptr, nullptr, parameter_ptr, &buf);

    // Release current run step resources
    JitRuntime::get()->ReleaseRunStep();

    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return hybridse::codec::Row();
    }
    return Row(base::RefCountedSlice::CreateManaged(
        buf, hybridse::codec::RowView::GetSize(buf)));
}

hybridse::codec::Row CoreAPI::RowProject(const RawPtrHandle fn,
                                         const hybridse::codec::Row& row,
                                         const hybridse::codec::Row& parameter,
                                         const bool need_free) {
    if (row.empty()) {
        return hybridse::codec::Row();
    }

    // Init current run step runtime
    JitRuntime::get()->InitRunStep();

    auto udf = reinterpret_cast<int32_t (*)(const int64_t, const int8_t*,
                                            const int8_t*, const int8_t*, int8_t**)>(
        const_cast<int8_t*>(fn));

    auto row_ptr = reinterpret_cast<const int8_t*>(&row);

    // TODO(tobe): do not need to pass parameter row for offline
    auto parameter_ptr = reinterpret_cast<const int8_t*>(&parameter);

    int8_t* buf = nullptr;
    uint32_t ret = udf(0, row_ptr, nullptr, parameter_ptr, &buf);

    // Release current run step resources
    JitRuntime::get()->ReleaseRunStep();

    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return hybridse::codec::Row();
    }

    return Row(base::RefCountedSlice::CreateManaged(
        buf, hybridse::codec::RowView::GetSize(buf)));
}

hybridse::codec::Row CoreAPI::UnsafeRowProject(
    const hybridse::vm::RawPtrHandle fn,
    hybridse::vm::ByteArrayPtr inputUnsafeRowBytes,
    const int inputRowSizeInBytes, const bool need_free) {
    // Create Row from input UnsafeRow bytes
    auto inputRow = Row(base::RefCountedSlice::Create(inputUnsafeRowBytes, inputRowSizeInBytes));

    return RowProject(fn, inputRow, Row(), need_free);
}

hybridse::codec::Row CoreAPI::UnsafeRowProjectDirect(
        const hybridse::vm::RawPtrHandle fn,
        hybridse::vm::NIOBUFFER inputUnsafeRowBytes,
        const int inputRowSizeInBytes, const bool need_free) {
    auto bufPtr = reinterpret_cast<int8_t *>(inputUnsafeRowBytes);

    // Create Row from input UnsafeRow bytes
    auto inputRow = Row(base::RefCountedSlice::Create(bufPtr, inputRowSizeInBytes));

    return RowProject(fn, inputRow, Row(), need_free);
}


void CoreAPI::CopyRowToUnsafeRowBytes(const hybridse::codec::Row& inputRow,
                                      hybridse::vm::ByteArrayPtr outputBytes,
                                      const int length) {
    memcpy(outputBytes, inputRow.buf() + codec::HEADER_LENGTH, length);
}

void CoreAPI::CopyRowToDirectByteBuffer(const hybridse::codec::Row& inputRow,
                                      hybridse::vm::NIOBUFFER outputBytes,
                                      const int length) {
    memcpy(outputBytes, inputRow.buf() + codec::HEADER_LENGTH, length);
}

hybridse::codec::Row CoreAPI::WindowProject(const RawPtrHandle fn,
                                            const uint64_t row_key,
                                            const Row& row,
                                            WindowInterface* window) {
    if (row.empty()) {
        return row;
    }
    // Init current run step runtime
    JitRuntime::get()->InitRunStep();

    auto udf = reinterpret_cast<int32_t (*)(const int64_t, const int8_t*,
                                            const int8_t*, const int8_t*, int8_t**)>(
        const_cast<int8_t*>(fn));
    int8_t* out_buf = nullptr;

    codec::ListRef<Row> window_ref;
    window_ref.list = reinterpret_cast<int8_t*>(window->GetWindow());
    auto window_ptr = reinterpret_cast<const int8_t*>(&window_ref);
    auto row_ptr = reinterpret_cast<const int8_t*>(&row);

    uint32_t ret =
        udf(static_cast<int64_t>(row_key), row_ptr, window_ptr, nullptr, &out_buf);

    // Release current run step resources
    JitRuntime::get()->ReleaseRunStep();

    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    return Row(base::RefCountedSlice::CreateManaged(out_buf,
                                                    RowView::GetSize(out_buf)));
}

hybridse::codec::Row CoreAPI::WindowProject(const RawPtrHandle fn,
                                            const uint64_t key, const Row& row,
                                            const bool is_instance,
                                            size_t append_slices,
                                            WindowInterface* window) {
    return Runner::WindowProject(fn, key, row, Row(), is_instance, append_slices,
                                 window->GetWindow());
}

hybridse::codec::Row CoreAPI::UnsafeWindowProject(
    const RawPtrHandle fn, const uint64_t key,
    hybridse::vm::ByteArrayPtr inputUnsafeRowBytes,
    const int inputRowSizeInBytes, const bool is_instance, size_t append_slices,
    WindowInterface* window) {

    // Create Row from input UnsafeRow bytes
    auto row = Row(base::RefCountedSlice::Create(inputUnsafeRowBytes, inputRowSizeInBytes));


    return Runner::WindowProject(fn, key, row, Row(), is_instance, append_slices,
                                 window->GetWindow());
}

hybridse::codec::Row CoreAPI::UnsafeWindowProjectDirect(
        const RawPtrHandle fn, const uint64_t key,
        hybridse::vm::NIOBUFFER inputUnsafeRowBytes,
        const int inputRowSizeInBytes, const bool is_instance, size_t append_slices,
        WindowInterface* window) {
    // Create Row from input UnsafeRow bytes
    // auto bufPtr = reinterpret_cast<int8_t *>(inputUnsafeRowBytes);
    // auto row = Row(base::RefCountedSlice::Create(bufPtr, inputRowSizeInBytes));

    // Notice that we need to use new pointer for buffering rows in window list
    int8_t* bufPtr = reinterpret_cast<int8_t*>(malloc(inputRowSizeInBytes));
    memcpy(bufPtr, inputUnsafeRowBytes, inputRowSizeInBytes);
    auto row = Row(base::RefCountedSlice::CreateManaged(bufPtr, inputRowSizeInBytes));

    return Runner::WindowProject(fn, key, row, Row(), is_instance, append_slices,
                                 window->GetWindow());
}

hybridse::codec::Row CoreAPI::UnsafeWindowProjectBytes(
        const RawPtrHandle fn, const uint64_t key,
        hybridse::vm::ByteArrayPtr unsaferowBytes,
        const int unsaferowSize, const bool is_instance, size_t append_slices,
        WindowInterface* window) {
    auto actualRowSize = unsaferowSize + codec::HEADER_LENGTH;
    int8_t* newRowPtr = reinterpret_cast<int8_t*>(malloc(actualRowSize));

    // Write the row size
    *reinterpret_cast<uint32_t *>(newRowPtr) = actualRowSize;

    // Write the UnsafeRow bytes
    memcpy(newRowPtr + codec::HEADER_LENGTH, unsaferowBytes, unsaferowSize);
    auto row = Row(base::RefCountedSlice::CreateManaged(newRowPtr, actualRowSize));

    return Runner::WindowProject(fn, key, row, Row(), is_instance, append_slices,
                                 window->GetWindow());
}

hybridse::codec::Row CoreAPI::GroupbyProject(
    const RawPtrHandle fn, hybridse::vm::GroupbyInterface* groupby_interface) {
    return Runner::GroupbyProject(fn, Row(), groupby_interface->GetTableHandler());
}

bool CoreAPI::ComputeCondition(const hybridse::vm::RawPtrHandle fn,
                               const Row& row,
                               const Row& parameter,
                               const hybridse::codec::RowView* row_view,
                               size_t out_idx) {
    Row cond_row = CoreAPI::RowProject(fn, row, parameter, true);
    return Runner::GetColumnBool(cond_row.buf(), row_view, out_idx,
                                 row_view->GetSchema()->Get(out_idx).type());
}

hybridse::codec::Row CoreAPI::NewRow(size_t bytes) {
    auto buf = reinterpret_cast<int8_t*>(malloc(bytes));
    if (buf == nullptr) {
        return hybridse::codec::Row();
    }
    auto slice = base::RefCountedSlice::CreateManaged(buf, bytes);
    return hybridse::codec::Row(slice);
}

RawPtrHandle CoreAPI::GetRowBuf(hybridse::codec::Row* row, size_t idx) {
    return row->buf(idx);
}

RawPtrHandle CoreAPI::AppendRow(hybridse::codec::Row* row, size_t bytes) {
    auto buf = reinterpret_cast<int8_t*>(malloc(bytes));
    if (buf == nullptr) {
        return nullptr;
    }
    auto slice = base::RefCountedSlice::CreateManaged(buf, bytes);
    row->Append(slice);
    return buf;
}

bool CoreAPI::EnableSignalTraceback() {
    signal(SIGSEGV, hybridse::base::FeSignalBacktraceHandler);
    signal(SIGBUS, hybridse::base::FeSignalBacktraceHandler);
    signal(SIGFPE, hybridse::base::FeSignalBacktraceHandler);
    signal(SIGILL, hybridse::base::FeSignalBacktraceHandler);
    signal(SIGSYS, hybridse::base::FeSignalBacktraceHandler);
    return true;
}

}  // namespace vm
}  // namespace hybridse
