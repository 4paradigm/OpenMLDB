/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * core_api.h
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_VM_CORE_API_H_
#define SRC_VM_CORE_API_H_

#include <map>
#include <memory>
#include <string>
#include "codec/fe_row_codec.h"
#include "codec/row.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"
#include "vm/physical_op.h"

namespace fesql {
namespace vm {

class CoreAPI;
class Window;
class FeSQLJITWrapper;

typedef const int8_t* RawPtrHandle;

class WindowInterface {
 public:
    WindowInterface(bool instance_not_in_window, int64_t start_offset,
                    int64_t end_offset, uint64_t row_preceding,
                    uint32_t max_size);

    void BufferData(uint64_t key, const Row& row);

    fesql::codec::Row Get(uint64_t idx) const { return window_impl_->At(idx); }

    size_t size() const { return window_impl_->GetCount(); }

 private:
    friend CoreAPI;

    Window* GetWindow() { return window_impl_.get(); }
    std::unique_ptr<Window> window_impl_;
};

class GroupbyInterface {
 public:
    explicit GroupbyInterface(const fesql::codec::Schema& schema);
    void AddRow(fesql::codec::Row* row);
    fesql::vm::TableHandler* GetTableHandler();

 private:
    friend CoreAPI;
    fesql::vm::MemTableHandler* mem_table_handler_;
};

class ColumnSourceInfo {
 public:
    fesql::base::Status GetStatus() const { return status_; }
    size_t GetColumnID() const { return column_id_; }
    int GetChildPathIndex() const { return child_path_idx_; }
    size_t GetChildColumnID() const { return child_column_id_; }
    size_t GetSourceColumnID() const { return source_column_id_; }
    const std::string& GetSourceColumnName() const { return source_col_name_; }
    const fesql::vm::PhysicalOpNode* GetSourceNode() const {
        return source_node_;
    }
    int GetColumnIndex() const { return total_col_idx_; }
    int GetColumnIndexInSlice() const { return col_idx_; }
    int GetSchemaIndex() const { return schema_idx_; }
    int GetSourceColumnIndex() const { return source_total_col_idx_; }
    int GetSourceColumnIndexInSlice() const { return source_col_idx_; }
    int GetSourceSchemaIndex() const { return source_schema_idx_; }

 private:
    friend class fesql::vm::CoreAPI;
    fesql::base::Status status_;
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
    static fesql::codec::Row* NewRow(size_t bytes);
    static RawPtrHandle GetRowBuf(fesql::codec::Row*, size_t idx);
    static RawPtrHandle AppendRow(fesql::codec::Row*, size_t bytes);

    static int ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                  fesql::node::ColumnRefNode* expr);

    static std::string ResolveSourceColumnName(
        fesql::vm::PhysicalOpNode* node, fesql::node::ColumnRefNode* expr);

    static ColumnSourceInfo ResolveSourceColumn(
        fesql::vm::PhysicalOpNode* node, const std::string& relation_name,
        const std::string& column_name);

    static size_t GetUniqueID(const fesql::vm::PhysicalOpNode* node);

    static fesql::codec::Row RowProject(const fesql::vm::RawPtrHandle fn,
                                        const fesql::codec::Row row,
                                        const bool need_free = false);
    static fesql::codec::Row RowConstProject(const fesql::vm::RawPtrHandle fn,
                                             const bool need_free = false);

    static fesql::codec::Row WindowProject(const fesql::vm::RawPtrHandle fn,
                                           const uint64_t key, const Row row,
                                           const bool is_instance,
                                           size_t append_slices,
                                           WindowInterface* window);
    static fesql::codec::Row WindowProject(const fesql::vm::RawPtrHandle fn,
                                           const Row row,
                                           WindowInterface* window);

    static fesql::codec::Row GroupbyProject(
        const fesql::vm::RawPtrHandle fn,
        fesql::vm::GroupbyInterface* groupby_interface);

    static bool ComputeCondition(const fesql::vm::RawPtrHandle fn,
                                 const Row& row,
                                 const fesql::codec::RowView* row_view,
                                 size_t out_idx);

    static bool EnableSignalTraceback();
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_CORE_API_H_
