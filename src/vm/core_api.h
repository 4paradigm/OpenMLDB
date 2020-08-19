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
#include "vm/physical_op.h"
#include "vm/simple_catalog.h"
#include "vm/mem_catalog.h"

namespace fesql {
namespace vm {

class CoreAPI;
class Window;

typedef const int8_t* RawPtrHandle;

class WindowInterface {
 public:
    WindowInterface(bool instance_not_in_window, int64_t start_offset,
                    int64_t end_offset, uint64_t row_preceding,
                    uint32_t max_size);

    void BufferData(uint64_t key, const Row& row);

 private:
    friend CoreAPI;

    Window* GetWindow() { return window_impl_.get(); }
    std::unique_ptr<Window> window_impl_;
};

class RunnerContext {
 public:
    explicit RunnerContext(const bool is_debug = false)
        : request_(), is_debug_(is_debug), cache_() {}
    explicit RunnerContext(const fesql::codec::Row& request,
                           const bool is_debug = false)
        : request_(request), is_debug_(is_debug), cache_() {}
    const fesql::codec::Row request_;
    const bool is_debug_;
    std::map<int32_t, std::shared_ptr<DataHandler>> cache_;
};

class CoreAPI {
 public:
    static fesql::codec::Row* NewRow(size_t bytes);
    static RawPtrHandle GetRowBuf(fesql::codec::Row*, size_t idx);
    static RawPtrHandle AppendRow(fesql::codec::Row*, size_t bytes);

    static fesql::vm::MemTableHandler* NewMemTableHandler(const std::string& table_name, const std::string& db, const fesql::codec::Schema& schema);
    static void AddRowToMemTable(fesql::vm::MemTableHandler* table_handler, fesql::codec::Row* row);

    static int ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                  fesql::node::ColumnRefNode* expr);
    static int ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                  int32_t schema_idx, int32_t column_idx);
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

    static fesql::codec::Row GroupbyProject(const fesql::vm::RawPtrHandle fn,
                                           fesql::vm::MemTableHandler* table);

    static bool ComputeCondition(const fesql::vm::RawPtrHandle fn,
                                 const Row& row,
                                 fesql::codec::RowView* row_view,
                                 size_t out_idx);
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_CORE_API_H_
