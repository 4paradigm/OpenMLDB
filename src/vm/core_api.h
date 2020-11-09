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

class GroupbyInterface {
 public:
    explicit GroupbyInterface(const fesql::codec::Schema& schema);
    void AddRow(fesql::codec::Row* row);
    fesql::vm::TableHandler* GetTableHandler();

 private:
    friend CoreAPI;
    fesql::vm::MemTableHandler* mem_table_handler_;
};

class RunnerContext {
 public:
    explicit RunnerContext(const bool is_debug = false)
        : request_(),
          is_debug_(is_debug),
          cache_(),
          catalog_() {}
    explicit RunnerContext(const fesql::codec::Row& request,
                           const bool is_debug = false)
        : request_(request),
          is_debug_(is_debug),
          cache_(),
          catalog_() {}

    RunnerContext(const fesql::codec::Row& request, bool is_debug,
                  std::shared_ptr<Catalog> catalog)
        : request_(request),
          is_debug_(is_debug),
          cache_(),
          catalog_(catalog) {}

    const fesql::codec::Row& request() const { return request_; }
    void SetRequest(const fesql::codec::Row& request);
    bool is_debug() const { return is_debug_; }

    std::shared_ptr<DataHandler> GetCache(int64_t id) const;
    void SetCache(int64_t id, std::shared_ptr<DataHandler>);
    std::shared_ptr<Catalog> GetCatalog() const { return catalog_; }

 private:
    fesql::codec::Row request_;
    const bool is_debug_;
    std::map<int64_t, std::shared_ptr<DataHandler>> cache_;
    std::shared_ptr<Catalog> catalog_;
};

class CoreAPI {
 public:
    static fesql::codec::Row* NewRow(size_t bytes);
    static RawPtrHandle GetRowBuf(fesql::codec::Row*, size_t idx);
    static RawPtrHandle AppendRow(fesql::codec::Row*, size_t bytes);

    static int ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                  fesql::node::ColumnRefNode* expr);
    static int ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                  int32_t schema_idx, int32_t column_idx);
    // 获取原始列名的接口
    static std::string ResolvedSourceColumnName(fesql::vm::PhysicalOpNode* node, fesql::node::ExprNode* expr);
    // 获取列名的接口
    static std::string ResolvedColumnName(fesql::vm::PhysicalOpNode* node, fesql::node::ExprNode* expr);
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

    static fesql::codec::Row GroupbyProject(
        const fesql::vm::RawPtrHandle fn,
        fesql::vm::GroupbyInterface* groupby_interface);

    static bool ComputeCondition(const fesql::vm::RawPtrHandle fn,
                                 const Row& row,
                                 fesql::codec::RowView* row_view,
                                 size_t out_idx);

    static bool EnableSignalTraceback();
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_CORE_API_H_
