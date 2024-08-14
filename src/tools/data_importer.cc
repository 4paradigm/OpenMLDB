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
#include <snappy.h>
#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include "base/file_util.h"
#include "base/taskpool.hpp"
#include "boost/bind.hpp"
#include "codec/codec.h"
#include "codec/sdk_codec.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "schema/index_util.h"
#include "sdk/db_sdk.h"
#include "sdk/sql_router.h"
#include "sdk/sql_cluster_router.h"

DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DEFINE_string(data_file, "", "config the data file");
DEFINE_int32(thread_num, 5, "");
DEFINE_string(table_name, "", "table name");
DEFINE_string(delimiter, ",", "delimiter");
DEFINE_bool(has_ts_col, true, "has ts col");
DEFINE_bool(use_client_side_compression, false, "whether use client side compression. For legacy versions, it should be set to true.");

std::string DEFAULT_DB = "default_db";

namespace openmldb {
namespace tools {

using ::openmldb::schema::PBSchema;

class DataImporter {
  public:
    DataImporter(const ::openmldb::nameserver::TableInfo& table_info,
            const std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>& tablets)
        : table_info_(table_info), tablets_(tablets), sdk_codec_(table_info),
        part_size_(table_info.table_partition_size()) {}

    void PutRecord(const std::string& record) {
        std::vector<std::string> fields;
        openmldb::base::SplitString(record, FLAGS_delimiter, fields);
        uint64_t ts = ::baidu::common::timer::get_micros() / 1000;
        int start_idx = 0;
        if (FLAGS_has_ts_col) {
            ts = boost::lexical_cast<uint64_t>(fields.front());
            start_idx++;
        }
        if ((int)fields.size() - start_idx != table_info_.column_desc_size()) {
            LOG(WARNING) << "fileds size is not match schema." << record;
            return;
        }
        std::map<uint32_t, ::openmldb::codec::Dimension> dimensions;
        if (sdk_codec_.EncodeDimension(fields, start_idx, part_size_, &dimensions) < 0) {
            LOG(WARNING) << "encode dimension failed. record is " << record;
            return;
        }
        std::string value;
        if (EncodeRow(fields, start_idx, value) < 0) {
            LOG(WARNING) << "encode row failed. record is " << record;
            return;
        }
        if (FLAGS_use_client_side_compression) {
            if (table_info_.compress_type() == ::openmldb::type::CompressType::kSnappy) {
                std::string compressed;
                ::snappy::Compress(value.c_str(), value.length(), &compressed);
                value.swap(compressed);
            }
        }
        for (const auto& kv : dimensions) {
            uint32_t pid = kv.first;
            auto client = tablets_[pid]->GetClient();
            if (!client->Put(table_info_.tid(), pid, ts, value, kv.second)) {
                return;
            }
        }
    }

    int EncodeRow(const std::vector<std::string>& input_value, int start_idx, std::string& row) {
        const auto& schema = table_info_.column_desc();
        int32_t str_len = CalStrLength(input_value, start_idx, schema);
        if (str_len < 0) {
            return -1;
        }
        ::openmldb::codec::RowBuilder builder(schema);
        uint32_t size = builder.CalTotalLength(str_len);
        builder.SetSchemaVersion(1);
        row.resize(size);
        builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
        for (int i = 0; i < schema.size(); i++) {
            const ::openmldb::common::ColumnDesc& col = schema.Get(i);
            const auto& value = input_value.at(i + start_idx);
            if (!col.not_null() && (value == "null" || value == ::openmldb::codec::NONETOKEN)) {
                builder.AppendNULL();
                continue;
            } else if (value == "null" || value == ::openmldb::codec::NONETOKEN) {
                return -1 ;
            }
            if (!builder.AppendValue(value)) {
                return -1;
            }
        }
        return 0;
    }

    int32_t CalStrLength(const std::vector<std::string>& input_value, int start_idx, const PBSchema& schema) {
        if ((int)(input_value.size() - start_idx) != schema.size()) {
            return -1;
        }
        int32_t str_len = 0;
        for (int i = 0; i < schema.size(); i++) {
            const ::openmldb::common::ColumnDesc& col = schema.Get(i);
            const auto& value = input_value.at(i + start_idx);
            if (col.data_type() == ::openmldb::type::kVarchar || col.data_type() == ::openmldb::type::kString) {
                if (!col.not_null() && (value == "null" || value == ::openmldb::codec::NONETOKEN)) {
                    continue;
                } else if (value == "null" || value == ::openmldb::codec::NONETOKEN) {
                    return -1;
                }
                str_len += value.length();
            }
        }
        return str_len;
    }
  private:
    //::openmldb::codec::RowBuilder builder_;
    ::openmldb::nameserver::TableInfo table_info_;
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets_;
    ::openmldb::codec::SDKCodec sdk_codec_;
    int part_size_;
};


}  // namespace tools
}  // namespace openmldb

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (!::openmldb::base::IsExists(FLAGS_data_file)) {
        printf("data file %s is not exist\n", FLAGS_data_file.c_str());
        return 0;
    }
    if (FLAGS_table_name.empty()) {
        printf("table_name should be set");
        return 0;
    }
    printf("thread_num %d\n", FLAGS_thread_num);
    ::openmldb::sdk::ClusterOptions copt;
    copt.zk_cluster = FLAGS_zk_cluster;
    copt.zk_path = FLAGS_zk_root_path;
    auto cs = new ::openmldb::sdk::ClusterSDK(copt);
    cs->Init();
    auto sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    sr->Init();
    auto table_info = sr->GetTableInfo(DEFAULT_DB, FLAGS_table_name);
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
    bool ret = cs->GetTablet(DEFAULT_DB, FLAGS_table_name, &tablets);
    if (!ret || tablets.empty()) {
        printf("fail to get tablet!\n");
        return 0;
    }
    std::ifstream fin;
    fin.open(FLAGS_data_file, std::ios::in);
    if (!fin.is_open()) {
        printf("fail to open file\n");
        return 0;
    }
    ::openmldb::tools::DataImporter data_importer(table_info, tablets);
    auto task_pool = std::make_shared<openmldb::base::TaskPool>(FLAGS_thread_num, 10000);
    task_pool->Start();
    std::string buff;
    // skip header
    getline(fin, buff);
    int count = 0;
    while (getline(fin, buff)) {
        if (!buff.empty()) {
            task_pool->AddTask(boost::bind(&::openmldb::tools::DataImporter::PutRecord, &data_importer, buff));
            //data_importer.PutRecord(buff);
        }
        count++;
        if (count % 1000000 == 0) {
            printf("load %d\n", count);
        }
    }
    task_pool->Stop();
    return 0;
}
