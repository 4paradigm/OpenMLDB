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

#ifndef HYBRIDSE_SRC_VM_CORE_API_H_
#define HYBRIDSE_SRC_VM_CORE_API_H_

#include <map>
#include <memory>
#include <string>
#include "codec/fe_row_codec.h"
#include "codec/row.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace vm {

class CoreAPI;
class Window;
class HybridSeJitWrapper;

typedef const int8_t* RawPtrHandle;
typedef int8_t* ByteArrayPtr;
typedef unsigned char *NIOBUFFER;

class WindowInterface {
 public:
    WindowInterface(bool instance_not_in_window, bool exclude_current_time, const std::string& frame_type_str,
                    int64_t start_offset, int64_t end_offset, uint64_t rows_preceding, uint64_t max_size);

    bool BufferData(uint64_t key, const Row& row);

    hybridse::codec::Row Get(uint64_t idx) const {
        return window_impl_->At(idx);
    }

    size_t size() const { return window_impl_->GetCount(); }

 private:
    friend CoreAPI;

    Window* GetWindow() { return window_impl_.get(); }
    inline Window::WindowFrameType ExtractFrameType(
        const std::string& frame_type_str) const;
    std::unique_ptr<Window> window_impl_;
};

class GroupbyInterface {
 public:
    explicit GroupbyInterface(const hybridse::codec::Schema& schema);
    void AddRow(hybridse::codec::Row* row);
    hybridse::vm::TableHandler* GetTableHandler();

 private:
    friend CoreAPI;
    hybridse::vm::MemTableHandler* mem_table_handler_;
};

class ColumnSourceInfo {
 public:
    hybridse::base::Status GetStatus() const { return status_; }
    size_t GetColumnID() const { return column_id_; }
    int GetChildPathIndex() const { return child_path_idx_; }
    size_t GetChildColumnID() const { return child_column_id_; }
    size_t GetSourceColumnID() const { return source_column_id_; }
    const std::string& GetSourceColumnName() const { return source_col_name_; }
    const hybridse::vm::PhysicalOpNode* GetSourceNode() const {
        return source_node_;
    }
    int GetColumnIndex() const { return total_col_idx_; }
    int GetColumnIndexInSlice() const { return col_idx_; }
    int GetSchemaIndex() const { return schema_idx_; }
    int GetSourceColumnIndex() const { return source_total_col_idx_; }
    int GetSourceColumnIndexInSlice() const { return source_col_idx_; }
    int GetSourceSchemaIndex() const { return source_schema_idx_; }

 private:
    friend class hybridse::vm::CoreAPI;
    hybridse::base::Status status_;
    size_t column_id_ = 0;
    int total_col_idx_ = -1;
    int col_idx_ = -1;
    int schema_idx_ = -1;
    int child_path_idx_ = -1;
    size_t child_column_id_ = 0;
    size_t source_column_id_ = 0;
    std::string source_col_name_ = "";
    int source_total_col_idx_ = -1;
    int source_col_idx_ = -1;
    int source_schema_idx_ = -1;
    const PhysicalOpNode* source_node_ = nullptr;
};

class CoreAPI {
 public:
    static hybridse::codec::Row NewRow(size_t bytes);
    static RawPtrHandle GetRowBuf(hybridse::codec::Row*, size_t idx);
    static RawPtrHandle AppendRow(hybridse::codec::Row*, size_t bytes);

    static int ResolveColumnIndex(hybridse::vm::PhysicalOpNode* node,
                                  hybridse::node::ExprNode* expr);

    static std::string ResolveSourceColumnName(
        hybridse::vm::PhysicalOpNode* node,
        hybridse::node::ColumnRefNode* expr);

    static ColumnSourceInfo ResolveSourceColumn(
        hybridse::vm::PhysicalOpNode* node, const std::string& db_name,
        const std::string& relation_name,
        const std::string& column_name);

    static size_t GetUniqueID(const hybridse::vm::PhysicalOpNode* node);

    static hybridse::codec::Row RowProject(const hybridse::vm::RawPtrHandle fn,
                                           const hybridse::codec::Row& row,
                                           const hybridse::codec::Row& parameter,
                                           const bool need_free = false);
    static hybridse::codec::Row RowConstProject(
        const hybridse::vm::RawPtrHandle fn, const hybridse::codec::Row parameter,
        const bool need_free = false);

    // Row project API with Spark UnsafeRow optimization
    static hybridse::codec::Row UnsafeRowProject(
        const hybridse::vm::RawPtrHandle fn,
        hybridse::vm::ByteArrayPtr inputUnsafeRowBytes,
        const int inputRowSizeInBytes, const bool need_free = false);

    static hybridse::codec::Row UnsafeRowProjectDirect(
            const hybridse::vm::RawPtrHandle fn,
            hybridse::vm::NIOBUFFER inputUnsafeRowBytes,
            const int inputRowSizeInBytes, const bool need_free = false);

    static void CopyRowToUnsafeRowBytes(const hybridse::codec::Row& inputRow,
                                        hybridse::vm::ByteArrayPtr outputBytes,
                                        const int length);

    static void CopyRowToDirectByteBuffer(const hybridse::codec::Row& inputRow,
                                        hybridse::vm::NIOBUFFER outputBytes,
                                        const int length);

    static hybridse::codec::Row WindowProject(
        const hybridse::vm::RawPtrHandle fn, const uint64_t key, const Row& row,
        const bool is_instance, size_t append_slices, WindowInterface* window);

    // Window project API with Spark UnsafeRow optimization
    static hybridse::codec::Row UnsafeWindowProject(
        const hybridse::vm::RawPtrHandle fn, const uint64_t key,
        hybridse::vm::ByteArrayPtr inputUnsafeRowBytes,
        const int inputRowSizeInBytes, const bool is_instance,
        size_t append_slices, WindowInterface* window);

    static hybridse::codec::Row UnsafeWindowProjectDirect(
            const hybridse::vm::RawPtrHandle fn, const uint64_t key,
            hybridse::vm::NIOBUFFER inputUnsafeRowBytes,
            const int inputRowSizeInBytes, const bool is_instance,
            size_t append_slices, WindowInterface* window);

    static hybridse::codec::Row UnsafeWindowProjectBytes(
            const hybridse::vm::RawPtrHandle fn, const uint64_t key,
            hybridse::vm::ByteArrayPtr unsaferowBytes,
            const int unsaferowSize, const bool is_instance,
            size_t append_slices, WindowInterface* window);

    static hybridse::codec::Row WindowProject(
        const hybridse::vm::RawPtrHandle fn, const uint64_t key, const Row& row,
        WindowInterface* window);

    static hybridse::codec::Row GroupbyProject(
        const hybridse::vm::RawPtrHandle fn,
        hybridse::vm::GroupbyInterface* groupby_interface);

    static bool ComputeCondition(const hybridse::vm::RawPtrHandle fn,
                                 const Row& row,
                                 const Row& parameter,
                                 const hybridse::codec::RowView* row_view,
                                 size_t out_idx);

    static bool EnableSignalTraceback();
};

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_VM_CORE_API_H_
