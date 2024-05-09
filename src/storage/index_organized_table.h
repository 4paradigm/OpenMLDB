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

#ifndef SRC_STORAGE_INDEX_ORGANIZED_TABLE_H_
#define SRC_STORAGE_INDEX_ORGANIZED_TABLE_H_

#include <mutex>

#include "catalog/tablet_catalog.h"
#include "storage/mem_table.h"

namespace openmldb::storage {

class IndexOrganizedTable : public MemTable {
 public:
    IndexOrganizedTable(const ::openmldb::api::TableMeta& table_meta, std::shared_ptr<catalog::TabletCatalog> catalog)
        : MemTable(table_meta), catalog_(catalog) {}

    TableIterator* NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) override;

    TraverseIterator* NewTraverseIterator(uint32_t index) override;

    ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t index) override;

    bool Init() override;

    bool Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) override;

    absl::Status Put(uint64_t time, const std::string& value, const Dimensions& dimensions,
                     bool put_if_absent) override;

    absl::Status CheckDataExists(uint64_t tsv, const Dimensions& dimensions);

    // TODO(hw): iot bulk load unsupported
    bool GetBulkLoadInfo(::openmldb::api::BulkLoadInfoResponse* response) { return false; }
    bool BulkLoad(const std::vector<DataBlock*>& data_blocks,
                  const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes) {
        return false;
    }
    bool AddIndexToTable(const std::shared_ptr<IndexDef>& index_def) override;

    void SchedGCByDelete(const std::shared_ptr<sdk::SQLRouter>& router);

    static std::map<std::string, std::pair<uint32_t, type::DataType>> MakePkeysHint(const codec::Schema& schema,
                                                                                    const common::ColumnKey& cidx_ck);
    static std::string MakeDeleteSQL(const std::string& db, const std::string& name, const common::ColumnKey& cidx_ck,
                                     const int8_t* values, uint64_t ts, const codec::RowView& row_view,
                                     const std::map<std::string, std::pair<uint32_t, type::DataType>>& col_idx);
    static std::string ExtractPkeys(const common::ColumnKey& cidx_ck, const int8_t* values,
                                    const codec::RowView& row_view,
                                    const std::map<std::string, std::pair<uint32_t, type::DataType>>& col_idx);

 private:
    absl::Status ClusteredIndexGCByDelete(const std::shared_ptr<sdk::SQLRouter>& router);

 private:
    // to get current distribute iterator
    std::shared_ptr<catalog::TabletCatalog> catalog_;

    std::mutex gc_lock_;
};
}  // namespace openmldb::storage

#endif
