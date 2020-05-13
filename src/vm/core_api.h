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
#include "codec/fe_row_codec.h"
#include "codec/row.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"
#include "vm/physical_op.h"

namespace fesql {
namespace vm {

class CoreAPI;

typedef const int8_t* RawFunctionPtr;

class WindowInterface {
 public:
    WindowInterface(int64_t start_offset, int64_t end_offset, uint32_t max_size)
        : window_impl_(std::unique_ptr<Window>(
              new CurrentHistoryWindow(start_offset, max_size))) {}
    WindowInterface(const bool instance_not_in_window, int64_t start_offset,
                    int64_t end_offset, uint32_t max_size)
        : window_impl_(std::unique_ptr<Window>(
              new CurrentHistoryWindow(start_offset, max_size))) {
        window_impl_->set_instance_not_in_window(instance_not_in_window);
    }
    void BufferData(uint64_t key, const Row& row) {
        window_impl_->BufferData(key, row);
    }

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
    static int ResolveColumnIndex(fesql::vm::PhysicalOpNode* node,
                                  fesql::node::ExprNode* expr);

    static fesql::codec::Row RowProject(const fesql::vm::RawFunctionPtr fn,
                                        const fesql::codec::Row row,
                                        const bool need_free = false);

    static fesql::codec::Row WindowProject(const fesql::vm::RawFunctionPtr fn,
                                           const uint64_t key, const Row row,
                                           const bool is_instance,
                                           WindowInterface* window);

    static bool ComputeCondition(const fesql::vm::RawFunctionPtr fn,
                                 const Row& row,
                                 fesql::codec::RowView* row_view,
                                 size_t out_idx);
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_CORE_API_H_
