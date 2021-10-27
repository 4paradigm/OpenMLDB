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
#ifndef SRC_BASE_DDL_PARSER_H_
#define SRC_BASE_DDL_PARSER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "codec/schema_codec.h"
#include "common/timer.h"
#include "proto/common.pb.h"
#include "proto/fe_type.pb.h"
#include "sdk/base_impl.h"
#include "sdk/sql_insert_row.h"
#include "vm/engine.h"
#include "vm/physical_op.h"
#include "vm/simple_catalog.h"

namespace openmldb::base {

using hybridse::vm::Catalog;
using hybridse::vm::DataProviderType;
using hybridse::vm::Filter;
using hybridse::vm::Join;
using hybridse::vm::Key;
using hybridse::vm::PhysicalOpNode;
using hybridse::vm::PhysicalOpType;
using hybridse::vm::SchemasContext;
using hybridse::vm::Sort;

using IndexMap = std::map<std::string, std::vector<::openmldb::common::ColumnKey>>;
std::ostream& operator<<(std::ostream& os, IndexMap& index_map);

class IndexMapBuilder {
 public:
    IndexMapBuilder() = default;
    // Create the index with unset TTLSt, return false if the index(same table, same keys, same ts) existed
    bool CreateIndex(const std::string& table, const hybridse::node::ExprListNode* keys,
                     const hybridse::node::OrderByNode* ts, const SchemasContext* ctx);
    bool UpdateIndex(const hybridse::vm::Range& range);
    // After ToMap, inner data will be cleared
    IndexMap ToMap();

 private:
    static std::vector<std::string> NormalizeColumns(const std::string& table,
                                                     const std::vector<hybridse::node::ExprNode*>& nodes,
                                                     const SchemasContext* ctx);
    // table, keys and ts -> table:key1,key2,...;ts
    static std::string Encode(const std::string& table, const hybridse::node::ExprListNode* keys,
                              const hybridse::node::OrderByNode* ts, const SchemasContext* ctx);

    static std::pair<std::string, common::ColumnKey> Decode(const std::string& index_str);

    static std::string GetTsCol(const std::string& index_str) {
        std::size_t ts_mark_pos = index_str.find(TS_MARK);
        if (ts_mark_pos == std::string::npos) {
            return {};
        }
        auto ts_begin = ts_mark_pos + 1;
        return index_str.substr(ts_begin);
    }

    static std::string GetTable(const std::string& index_str) {
        auto key_sep = index_str.find(KEY_MARK);
        if (key_sep == std::string::npos) {
            return {};
        }
        return index_str.substr(0, key_sep);
    }

 private:
    static constexpr int64_t MIN_TIME = 60 * 1000;
    static constexpr char KEY_MARK = ':';
    static constexpr char KEY_SEP = ',';
    static constexpr char TS_MARK = ';';

    std::string latest_record_;
    // map<table_keys_and_order_str, ttl_st>
    std::map<std::string, common::TTLSt*> index_map_;
};

// no plan_ctx_, node_manager_: we assume that creating new op won't affect the upper level structure.
class GroupAndSortOptimizedParser {
 public:
    GroupAndSortOptimizedParser() = default;

    // LRD
    void Parse(PhysicalOpNode* cur_op) {
        LOG_ASSERT(cur_op != nullptr);
        // just parse, won't modify, but need to cast ptr, so we use non-const producers.
        auto& producers = cur_op->producers();
        for (auto& producer : producers) {
            Parse(producer);
        }

        DLOG(INFO) << "parse " << hybridse::vm::PhysicalOpTypeName(cur_op->GetOpType());
        TransformParse(cur_op);
    }

    IndexMap GetIndexes() { return index_map_builder_.ToMap(); }

 private:
    // recursive parse, return true iff kProviderTypeTable optimized
    // new_in is useless, but we keep it, GroupAndSortOptimizedParser will be more similar to GroupAndSortOptimized.
    bool KeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* left_key, Key* index_key,
                            Key* right_key, Sort* sort, PhysicalOpNode** new_in);

    bool KeysAndOrderFilterOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
                                          Key* hash, Sort* sort, PhysicalOpNode** new_in) {
        return KeysOptimizedParse(root_schemas_ctx, in, group, hash, nullptr, sort, new_in);
    }

    bool JoinKeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Join* join,
                                PhysicalOpNode** new_in) {
        if (nullptr == join) {
            return false;
        }
        return FilterAndOrderOptimizedParse(root_schemas_ctx, in, join, &join->right_sort_, new_in);
    }
    bool FilterAndOrderOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
                                      Sort* sort, PhysicalOpNode** new_in) {
        return KeysOptimizedParse(root_schemas_ctx, in, &filter->left_key_, &filter->index_key_, &filter->right_key_,
                                  sort, new_in);
    }
    bool FilterOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
                              PhysicalOpNode** new_in) {
        return FilterAndOrderOptimizedParse(root_schemas_ctx, in, filter, nullptr, new_in);
    }
    bool KeyAndOrderOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group, Sort* sort,
                                   PhysicalOpNode** new_in) {
        Key mock_key;
        return KeysAndOrderFilterOptimizedParse(root_schemas_ctx, in, group, &mock_key, sort, new_in);
    }
    bool GroupOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
                             PhysicalOpNode** new_in) {
        return KeyAndOrderOptimizedParse(root_schemas_ctx, in, group, nullptr, new_in);
    }

    static std::vector<PhysicalOpNode*> InitJoinList(hybridse::vm::PhysicalWindowAggrerationNode* op);

    void TransformParse(PhysicalOpNode* in);

 private:
    IndexMapBuilder index_map_builder_;
};

class DDLParser {
 public:
    static constexpr const char* DB_NAME = "ddl_parser_db";

    static IndexMap ExtractIndexes(const std::string& sql, const ::hybridse::type::Database& db) {
        hybridse::vm::RequestRunSession session;
        return ExtractIndexes(sql, db, &session);
    }

    static IndexMap ExtractIndexes(
        const std::string& sql,
        const std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>>& schemas) {
        ::hybridse::type::Database db;
        std::string tmp_db = "temp_" + std::to_string(::baidu::common::timer::get_micros() / 1000);
        db.set_name(tmp_db);
        AddTables(schemas, &db);
        return ExtractIndexes(sql, db);
    }

    static IndexMap ExtractIndexesForBatch(const std::string& sql, const ::hybridse::type::Database& db) {
        hybridse::vm::BatchRunSession session;
        return ExtractIndexes(sql, db, &session);
    }

    static std::string Explain(const std::string& sql, const ::hybridse::type::Database& db) {
        hybridse::vm::RequestRunSession session;
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        catalog->AddDatabase(db);
        if (!GetPlan(sql, catalog, db.name(), &session)) {
            LOG(ERROR) << "sql get plan failed";
            return {};
        }
        std::ostringstream plan_oss;
        session.GetCompileInfo()->DumpPhysicalPlan(plan_oss, "\t");
        return plan_oss.str();
    }

 private:
    // tables are in one db, and db name will be rewritten for simplicity
    static IndexMap ExtractIndexes(const std::string& sql, const hybridse::type::Database& db,
                                   hybridse::vm::RunSession* session) {
        // To show index-based-optimization -> IndexSupport() == true -> whether to do LeftJoinOptimized
        // tablet catalog supports index, so we should add index support too
        auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
        auto cp = db;
        cp.set_name(DB_NAME);
        catalog->AddDatabase(cp);

        if (!GetPlan(sql, catalog, cp.name(), session)) {
            LOG(ERROR) << "sql get plan failed";
            return {};
        }
        auto compile_info = session->GetCompileInfo();
        auto plan = session->GetCompileInfo()->GetPhysicalPlan();
        return ParseIndexes(const_cast<hybridse::vm::PhysicalOpNode*>(plan));
    }

    // DLR
    static IndexMap ParseIndexes(hybridse::vm::PhysicalOpNode* node) {
        // This physical plan is optimized, but no real optimization about index(cuz no index in fake catalog).
        // So we can run GroupAndSortOptimizedParser on the plan(very like transformer's pass-ApplyPasses)
        GroupAndSortOptimizedParser parser;
        parser.Parse(node);
        return parser.GetIndexes();
    }

    static bool GetPlan(const std::string& sql, const std::shared_ptr<Catalog>& catalog, const std::string& db,
                        hybridse::vm::RunSession* session) {
        // TODO(hw): engine is input, do not create in here
        ::hybridse::vm::Engine::InitializeGlobalLLVM();
        ::hybridse::vm::EngineOptions options;
        options.set_keep_ir(true);
        options.set_compile_only(true);
        session->SetPerformanceSensitive(false);
        auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);

        ::hybridse::base::Status status;
        auto ok = engine->Get(sql, db, *session, status);
        if (!(ok && status.isOK())) {
            LOG(WARNING) << "hybrid engine compile sql failed, " << status.str();
            return false;
        }
        return true;
    }

    static void AddTables(
        const std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>>& schema,
        hybridse::type::Database* db) {
        for (auto& table : schema) {
            // add to database
            auto def = db->add_tables();
            def->set_name(table.first);
            auto& cols = table.second;
            for (auto& col : cols) {
                auto add = def->add_columns();
                add->set_name(col.name());
                add->set_type(codec::SchemaCodec::ConvertType(col.data_type()));
            }
        }
    }
};

}  // namespace openmldb::base

#endif  // SRC_BASE_DDL_PARSER_H_
