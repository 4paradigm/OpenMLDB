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
    WindowInterface(bool instance_not_in_window, fesql::vm::Range* range);

    bool BufferData(uint64_t key, const Row& row);

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

class CoreAPI {
 public:
    static fesql::codec::Row* NewRow(size_t bytes);
    static RawPtrHandle GetRowBuf(fesql::codec::Row*, size_t idx);
    static RawPtrHandle AppendRow(fesql::codec::Row*, size_t bytes);

    static int ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                  fesql::node::ColumnRefNode* expr);

    static std::string ResolveSourceColumnName(
        fesql::vm::PhysicalOpNode* node, fesql::node::ColumnRefNode* expr);

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
                                           const uint64_t key, const Row row,
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
