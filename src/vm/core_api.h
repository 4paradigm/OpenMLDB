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
#include "codec/row.h"
#include "vm/catalog.h"
namespace fesql {
namespace vm {
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
    static fesql::codec::Row RowProject(const int8_t* fn,
                                        const fesql::codec::Row row,
                                        const bool need_free = false);
};
}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_CORE_API_H_
