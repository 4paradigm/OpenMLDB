/*
 * engine_context.h
 * Copyright (C) 4paradigm 2021 chenjing <chenjing@4paradigm.com>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef HYBRIDSE_INCLUDE_VM_ENGINE_CONTEXT_H_
#define HYBRIDSE_INCLUDE_VM_ENGINE_CONTEXT_H_
#include <map>
#include <memory>
#include <set>
#include <string>
#include "boost/compute/detail/lru_cache.hpp"
#include "vm/physical_op.h"
namespace hybridse {
namespace vm {

enum EngineMode { kBatchMode, kRequestMode, kMockRequestMode, kBatchRequestMode, kOffline };
std::string EngineModeName(EngineMode mode);
absl::StatusOr<EngineMode> UnparseEngineMode(absl::string_view);

struct BatchRequestInfo {
    // common column indices in batch request mode
    std::set<size_t> common_column_indices;

    // common physical node ids during batch request
    std::set<size_t> common_node_set;

    // common output column indices
    std::set<size_t> output_common_column_indices;
};

class IndexHintHandler {
 public:
    virtual ~IndexHintHandler() {}
    // report a index hint.
    //
    // a index hint is determined by a few things:
    // 1. source database & source table: where possible index will create upon
    // 2. keys and ts: suggested index info
    // 3. epxr node: referring to the physical node contains possible optimizable expression,
    //    e.g. a ReqeustJoin node contains the join condition 't1.key = t2.key' that may do optimize
    //
    // TODO(ace): multiple index suggestion ? choose one.
    // Say a join condition: 't1.key1 = t2.key1 and t1.key2 = t2.keys', those indexes all sufficient for t2:
    // 1. key = key1
    // 2. key = key2
    // 3. key = key1 + key2
    virtual void Report(absl::string_view db, absl::string_view table, absl::Span<std::string const> keys,
                        absl::string_view ts, const PhysicalOpNode* expr_node) = 0;
};

enum ComileType {
    kCompileSql,
};
class CompileInfo {
 public:
    CompileInfo() {}
    virtual ~CompileInfo() {}
    virtual bool GetIRBuffer(const base::RawBuffer& buf) = 0;
    virtual size_t GetIRSize() = 0;
    virtual const EngineMode GetEngineMode() const = 0;
    virtual const std::string& GetSql() const = 0;
    virtual const Schema& GetSchema() const = 0;
    virtual const ComileType GetCompileType() const = 0;
    virtual const std::string& GetEncodedSchema() const = 0;
    virtual const Schema& GetRequestSchema() const = 0;
    virtual const Schema& GetParameterSchema() const = 0;
    virtual const std::string& GetRequestName() const = 0;
    virtual const std::string& GetRequestDbName() const = 0;
    virtual const hybridse::vm::BatchRequestInfo& GetBatchRequestInfo() const = 0;
    virtual const hybridse::vm::PhysicalOpNode* GetPhysicalPlan() const = 0;
    virtual void DumpPhysicalPlan(std::ostream& output, const std::string& tab) = 0;
    virtual void DumpClusterJob(std::ostream& output, const std::string& tab) = 0;
};

/// @typedef EngineLRUCache
/// - EngineMode
///     - DB name
///       - SQL string
///           - CompileInfo
typedef std::map<EngineMode,
                std::map<std::string,
                    boost::compute::detail::lru_cache<std::string, std::shared_ptr<CompileInfo>>>>
    EngineLRUCache;

class CompileInfoCache {
 public:
    virtual ~CompileInfoCache() {}

    virtual std::shared_ptr<hybridse::vm::CompileInfo> GetRequestInfo(
        const std::string& db, const std::string& sp_name,
        base::Status& status) = 0;  // NOLINT
    virtual std::shared_ptr<hybridse::vm::CompileInfo> GetBatchRequestInfo(
        const std::string& db, const std::string& sp_name,
        base::Status& status) = 0;  // NOLINT
};

class JitOptions {
 public:
    bool IsEnableMcjit() const { return enable_mcjit_; }
    void SetEnableMcjit(bool flag) { enable_mcjit_ = flag; }

    bool IsEnableVtune() const { return enable_vtune_; }
    void SetEnableVtune(bool flag) { enable_vtune_ = flag; }

    bool IsEnableGdb() const { return enable_gdb_; }
    void SetEnableGdb(bool flag) { enable_gdb_ = flag; }

    bool IsEnablePerf() const { return enable_perf_; }
    void SetEnablePerf(bool flag) { enable_perf_ = flag; }

 private:
    bool enable_mcjit_ = false;
    bool enable_vtune_ = false;
    bool enable_gdb_ = false;
    bool enable_perf_ = false;
};
}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_VM_ENGINE_CONTEXT_H_
