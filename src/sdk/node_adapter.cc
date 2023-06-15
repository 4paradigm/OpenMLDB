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

#include "sdk/node_adapter.h"

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/ddl_parser.h"
#include "codec/schema_codec.h"
#include "plan/plan_api.h"
#include "schema/schema_adapter.h"

DECLARE_uint32(partition_num);

namespace openmldb::sdk {

using hybridse::plan::PlanAPI;

hybridse::sdk::Status NodeAdapter::ExtractDeleteOption(
        const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& indexs,
        const std::vector<Condition>& condition_vec,
        DeleteOption* option) {
    std::string con_ts_name;
    for (auto idx = 0; idx < indexs.size(); idx++) {
        const auto& column_key = indexs.Get(idx);
        if (column_key.flag() != 0) {
            continue;
        }
        std::string pk;
        int match_index_col = 0;
        for (const auto& col : column_key.col_name()) {
            for (const auto& con : condition_vec) {
                if (con.col_name == col) {
                    if (con.op != hybridse::node::FnOperator::kFnOpEq) {
                        return {hybridse::common::StatusCode::kCmdError, "only support equal condition on index col"};
                    }
                    match_index_col++;
                    if (!pk.empty()) {
                        pk.append("|");
                    }
                    if (!con.val.has_value()) {
                        pk.append(hybridse::codec::NONETOKEN);
                    } else if (con.val.value().empty()) {
                        pk.append(hybridse::codec::EMPTY_STRING);
                    } else {
                        pk.append(con.val.value());
                    }
                }
            }
        }
        if (match_index_col > 0) {
            if (match_index_col != column_key.col_name_size()) {
                continue;
            }
            option->index_map.emplace(idx, pk);
        }
        if (column_key.has_ts_name()) {
            for (const auto& con : condition_vec) {
                if (con.col_name != column_key.ts_name()) {
                    continue;
                }
                if (con_ts_name.empty()) {
                    con_ts_name = con.col_name;
                } else if (con.col_name != con_ts_name) {
                    return {hybridse::common::StatusCode::kCmdError,
                        "setting more than one ts column is not allowed in delete statement"};
                }
                if (!con.val.has_value()) {
                    return {hybridse::common::StatusCode::kCmdError, "ts column cannot be null"};
                }
                uint64_t ts = 0;
                if (!absl::SimpleAtoi(con.val.value(), &ts)) {
                    return {hybridse::common::StatusCode::kCmdError,
                        absl::StrCat("cannot convert to integer! value ", con.val.value())};
                }
                if (con.op == hybridse::node::FnOperator::kFnOpEq) {
                    option->start_ts = ts;
                    if (ts > 0) {
                        option->end_ts = ts - 1;
                    }
                } else if (con.op == hybridse::node::FnOperator::kFnOpLe) {
                    option->start_ts = ts;
                } else if (con.op == hybridse::node::FnOperator::kFnOpLt) {
                    if (ts == 0) {
                        return {hybridse::common::StatusCode::kCmdError, "invalid condition with ts column"};
                    }
                    option->start_ts = ts - 1;;
                } else if (con.op == hybridse::node::FnOperator::kFnOpGt && ts != 0) {
                    option->end_ts = ts;
                } else if (con.op == hybridse::node::FnOperator::kFnOpGe && ts > 1) {
                    option->end_ts = ts - 1;
                }
            }
        }
    }
    return {};
}

hybridse::sdk::Status NodeAdapter::CheckCondition(
        const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& indexs,
        const std::vector<Condition>& condition_vec) {
    std::string con_ts_name;
    bool hit_index = false;
    for (auto idx = 0; idx < indexs.size(); idx++) {
        const auto& column_key = indexs.Get(idx);
        if (column_key.flag() != 0) {
            continue;
        }
        int match_index_col = 0;
        for (const auto& col : column_key.col_name()) {
            for (const auto& con : condition_vec) {
                if (con.col_name == col) {
                    if (con.op != hybridse::node::FnOperator::kFnOpEq) {
                        return {hybridse::common::StatusCode::kCmdError, "only support equal condition on index col"};
                    }
                    match_index_col++;
                }
            }
        }
        if (match_index_col > 0) {
            if (match_index_col != column_key.col_name_size()) {
                continue;
            }
            hit_index = true;
        }
        if (column_key.has_ts_name()) {
            for (const auto& con : condition_vec) {
                if (con.col_name != column_key.ts_name()) {
                    continue;
                }
                if (con_ts_name.empty()) {
                    con_ts_name = con.col_name;
                } else if (con.col_name != con_ts_name) {
                    return {hybridse::common::StatusCode::kCmdError,
                        "setting more than one ts column is not allowed in delete statement"};
                }
                if (!con.val.has_value()) {
                    return {hybridse::common::StatusCode::kCmdError, "ts column cannot be null"};
                }
            }
        }
    }
    if (!hit_index && con_ts_name.empty()) {
        return {hybridse::common::StatusCode::kCmdError, "no index or ts columnhit"};
    }
    return {};
}

bool NodeAdapter::TransformToTableDef(::hybridse::node::CreatePlanNode* create_node,
                                      ::openmldb::nameserver::TableInfo* table, uint32_t default_replica_num,
                                      bool is_cluster_mode, hybridse::base::Status* status) {
    if (create_node == nullptr || table == nullptr || status == nullptr) return false;
    std::string table_name = create_node->GetTableName();
    const hybridse::node::NodePointVector& column_desc_list = create_node->GetColumnDescList();
    const hybridse::node::NodePointVector& table_option_list = create_node->GetTableOptionList();
    table->set_name(table_name);
    hybridse::node::NodePointVector distribution_list;

    hybridse::node::StorageMode storage_mode = hybridse::node::kMemory;
    // different default value for cluster and standalone mode
    int replica_num = 1;
    int partition_num = 1;
    bool setted_replica_num = false;
    bool setted_partition_num = false;
    if (is_cluster_mode) {
        replica_num = default_replica_num;
        partition_num = FLAGS_partition_num;
    }
    // resolve table_option_list
    for (auto& table_option : table_option_list) {
        if (table_option != nullptr) {
            switch (table_option->GetType()) {
                case hybridse::node::kReplicaNum: {
                    replica_num = dynamic_cast<hybridse::node::ReplicaNumNode *>(table_option)->GetReplicaNum();
                    setted_replica_num = true;
                    break;
                }
                case hybridse::node::kPartitionNum: {
                    partition_num =
                        dynamic_cast<hybridse::node::PartitionNumNode*>(table_option)->GetPartitionNum();
                    setted_partition_num = true;
                    break;
                }
                case hybridse::node::kStorageMode: {
                    storage_mode = dynamic_cast<hybridse::node::StorageModeNode *>(table_option)->GetStorageMode();
                    break;
                }
                case hybridse::node::kDistributions: {
                    distribution_list =
                        dynamic_cast<hybridse::node::DistributionsNode*>(table_option)->GetDistributionList();
                    break;
                }
                default: {
                    LOG(WARNING) << "can not handle type " << NameOfSqlNodeType(table_option->GetType())
                                << " for table node";
                }
            }
        }
    }
    if (replica_num <= 0) {
        *status = {hybridse::common::kUnsupportSql, "replicanum should be great than 0"};
        return false;
    }
    if (partition_num <= 0) {
        *status = {hybridse::common::kUnsupportSql, "partitionnum should be great than 0"};
        return false;
    }
    // deny create table when invalid configuration in standalone mode
    if (!is_cluster_mode) {
        if (replica_num != 1) {
            status->msg = "Fail to create table with the replica configuration in standalone mode";
            status->code = hybridse::common::kUnsupportSql;
            return false;
        }
        if (!distribution_list.empty()) {
            status->msg = "Fail to create table with the distribution configuration in standalone mode";
            status->code = hybridse::common::kUnsupportSql;
            return false;
        }
    }
    table->set_replica_num(replica_num);
    table->set_partition_num(partition_num);
    table->set_storage_mode(static_cast<common::StorageMode>(storage_mode));
    bool has_generate_index = false;
    std::set<std::string> index_names;
    std::map<std::string, ::openmldb::common::ColumnDesc*> column_names;
    for (auto column_desc : column_desc_list) {
        switch (column_desc->GetType()) {
            case hybridse::node::kColumnDesc: {
                auto* column_def = dynamic_cast<hybridse::node::ColumnDefNode*>(column_desc);
                ::openmldb::common::ColumnDesc* add_column_desc = table->add_column_desc();
                if (column_names.find(add_column_desc->name()) != column_names.end()) {
                    status->msg = "CREATE common: COLUMN NAME " + column_def->GetColumnName() + " duplicate";
                    status->code = hybridse::common::kUnsupportSql;
                    return false;
                }
                add_column_desc->set_name(column_def->GetColumnName());
                add_column_desc->set_not_null(column_def->GetIsNotNull());
                column_names.insert(std::make_pair(column_def->GetColumnName(), add_column_desc));
                openmldb::type::DataType data_type;
                if (!openmldb::schema::SchemaAdapter::ConvertType(column_def->GetColumnType(), &data_type)) {
                    status->msg = "CREATE common: column type " +
                                  hybridse::node::DataTypeName(column_def->GetColumnType()) + " is not supported";
                    status->code = hybridse::common::kUnsupportSql;
                    return false;
                }
                add_column_desc->set_data_type(data_type);
                auto default_val = column_def->GetDefaultValue();
                if (default_val) {
                    if (default_val->GetExprType() != hybridse::node::kExprPrimary) {
                        status->msg = "CREATE common: default value expression not supported";
                        status->code = hybridse::common::kTypeError;
                        return false;
                    }
                    auto val = TransformDataType(*dynamic_cast<hybridse::node::ConstNode*>(default_val),
                                                 add_column_desc->data_type());
                    if (!val) {
                        status->msg = "CREATE common: default value type mismatch";
                        status->code = hybridse::common::kTypeError;
                        return false;
                    }
                    add_column_desc->set_default_value(DataToString(*val));
                }
                break;
            }

            case hybridse::node::kColumnIndex: {
                auto* column_index = dynamic_cast<hybridse::node::ColumnIndexNode*>(column_desc);
                std::string index_name = column_index->GetName();
                // index in `create table` won't set name
                DCHECK(index_name.empty());
                index_name = PlanAPI::GenerateName("INDEX", table->column_key_size());
                if (index_names.find(index_name) != index_names.end()) {
                    status->msg = "CREATE common: INDEX NAME " + index_name + " duplicate";
                    status->code = hybridse::common::kUnsupportSql;
                    return false;
                }
                ::openmldb::common::ColumnKey* index = table->add_column_key();
                if (column_index->GetKey().empty()) {
                    if (!has_generate_index && !column_index->GetTs().empty()) {
                        const auto& ts_name = column_index->GetTs();
                        for (const auto& col : table->column_desc()) {
                            if (col.name() != ts_name && col.data_type() != openmldb::type::DataType::kFloat &&
                                col.data_type() != openmldb::type::DataType::kDouble) {
                                index->add_col_name(col.name());
                                has_generate_index = true;
                                break;
                            }
                        }
                        if (!has_generate_index) {
                            status->msg = "CREATE common: can not found index col";
                            status->code = hybridse::common::kUnsupportSql;
                            return false;
                        }
                    } else {
                        status->msg = "CREATE common: INDEX KEY empty";
                        status->code = hybridse::common::kUnsupportSql;
                        return false;
                    }
                }
                index_names.insert(index_name);
                column_index->SetName(index_name);
                if (!TransformToColumnKey(column_index, column_names, index, status)) {
                    return false;
                }
                break;
            }

            default: {
                status->msg = "can not support " + hybridse::node::NameOfSqlNodeType(column_desc->GetType()) +
                              " when CREATE TABLE";
                status->code = hybridse::common::kUnsupportSql;
                return false;
            }
        }
    }
    if (!distribution_list.empty()) {
        int cur_replica_num = 0;
        for (size_t idx = 0; idx < distribution_list.size(); idx++) {
            auto table_partition = table->add_table_partition();
            table_partition->set_pid(idx);
            auto partition_mata_nodes = dynamic_cast<hybridse::node::SqlNodeList*>(distribution_list.at(idx));
            if (idx == 0) {
                cur_replica_num = partition_mata_nodes->GetSize();
            } else if (cur_replica_num != partition_mata_nodes->GetSize()) {
                *status = {hybridse::common::kUnsupportSql, "replica num is inconsistency"};
                return false;
            }
            std::set<std::string> endpoint_set;
            for (auto partition_meta : partition_mata_nodes->GetList()) {
                switch (partition_meta->GetType()) {
                    case hybridse::node::kPartitionMeta: {
                        auto p_meta_node = dynamic_cast<hybridse::node::PartitionMetaNode*>(partition_meta);
                        const std::string& ep = p_meta_node->GetEndpoint();
                        if (endpoint_set.count(ep) > 0) {
                            status->msg = "CREATE common: partition meta endpoint duplicate";
                            status->code = hybridse::common::kUnsupportSql;
                            return false;
                        }
                        endpoint_set.insert(ep);
                        auto meta = table_partition->add_partition_meta();
                        meta->set_endpoint(ep);
                        if (p_meta_node->GetRoleType() == hybridse::node::kLeader) {
                            meta->set_is_leader(true);
                        } else if (p_meta_node->GetRoleType() == hybridse::node::kFollower) {
                            meta->set_is_leader(false);
                        } else {
                            status->msg = "CREATE common: role_type " +
                                          hybridse::node::RoleTypeName(p_meta_node->GetRoleType()) + " not support";
                            status->code = hybridse::common::kUnsupportSql;
                            return false;
                        }
                        break;
                    }
                    default: {
                        status->msg = "can not support " + hybridse::node::NameOfSqlNodeType(partition_meta->GetType())
                                        + " when CREATE TABLE";
                        status->code = hybridse::common::kUnsupportSql;
                        return false;
                    }
                }
            }
        }
        if (setted_partition_num && table->partition_num() != distribution_list.size()) {
            *status = {hybridse::common::kUnsupportSql, "distribution_list size and partition_num is not match"};
            return false;
        }
        table->set_partition_num(distribution_list.size());
        if (setted_replica_num && static_cast<int>(table->replica_num()) != cur_replica_num) {
            *status = {hybridse::common::kUnsupportSql, "replica in distribution_list and replica_num is not match"};
            return false;
        }
        table->set_replica_num(cur_replica_num);
    }
    return true;
}

// If column_names is not empty, check the column key names
bool NodeAdapter::TransformToColumnKey(hybridse::node::ColumnIndexNode* column_index,
                                       const std::map<std::string, ::openmldb::common::ColumnDesc*>& column_names,
                                       common::ColumnKey* index, hybridse::base::Status* status) {
    if (column_index == nullptr) {
        return false;
    }

    std::stringstream ss;
    column_index->Print(ss, "");
    DLOG(INFO) << ss.str();
    index->set_index_name(column_index->GetName());

    for (const auto& key : column_index->GetKey()) {
        index->add_col_name(key);
    }
    // if no column_names, skip check
    if (!column_names.empty()) {
        for (const auto& col : index->col_name()) {
            if (column_names.find(col) == column_names.end()) {
                status->msg = "column " + col + " does not exist";
                status->code = hybridse::common::kUnsupportSql;
                return false;
            }
        }
    }
    ::openmldb::common::TTLSt* ttl_st = index->mutable_ttl();
    if (!column_index->ttl_type().empty()) {
        std::string ttl_type = column_index->ttl_type();
        std::transform(ttl_type.begin(), ttl_type.end(), ttl_type.begin(), ::tolower);
        openmldb::type::TTLType type;
        if (!::openmldb::codec::SchemaCodec::TTLTypeParse(ttl_type, &type)) {
            status->msg = "CREATE common: ttl_type " + column_index->ttl_type() + " not support";
            status->code = hybridse::common::kUnsupportSql;
            return false;
        }
        ttl_st->set_ttl_type(type);
    } else {
        ttl_st->set_ttl_type(openmldb::type::kAbsoluteTime);
    }
    if (ttl_st->ttl_type() == openmldb::type::kAbsoluteTime) {
        if (column_index->GetAbsTTL() == -1 || column_index->GetLatTTL() != -2) {
            status->msg = "CREATE common: abs ttl format error or set lat ttl";
            status->code = hybridse::common::kUnsupportSql;
            return false;
        }
        if (column_index->GetAbsTTL() == -2) {
            ttl_st->set_abs_ttl(0);
        } else {
            // set abs_ttl to 0, it means no gc
            // otherwise, convert it(ms) to minutes, >= 1 min
            ttl_st->set_abs_ttl(base::AbsTTLConvert(column_index->GetAbsTTL(), true));
        }
    } else if (ttl_st->ttl_type() == openmldb::type::kLatestTime) {
        if (column_index->GetLatTTL() == -1 || column_index->GetAbsTTL() != -2) {
            status->msg = "CREATE common: lat ttl format error";
            status->code = hybridse::common::kUnsupportSql;
            return false;
        }
        if (column_index->GetLatTTL() == -2) {
            ttl_st->set_lat_ttl(0);
        } else {
            // latest 0 also means no gc
            ttl_st->set_lat_ttl(base::LatTTLConvert(column_index->GetLatTTL(), true));
        }
    } else {
        if (column_index->GetAbsTTL() == -1) {
            status->msg = "CREATE common: abs ttl format error for " + type::TTLType_Name(ttl_st->ttl_type());
            status->code = hybridse::common::kUnsupportSql;
            return false;
        }
        if (column_index->GetAbsTTL() == -2) {
            ttl_st->set_abs_ttl(0);
        } else {
            ttl_st->set_abs_ttl(base::AbsTTLConvert(column_index->GetAbsTTL(), true));
        }
        if (column_index->GetLatTTL() == -1) {
            status->msg = "CREATE common: lat ttl format error for " + type::TTLType_Name(ttl_st->ttl_type());
            status->code = hybridse::common::kUnsupportSql;
            return false;
        }
        if (column_index->GetLatTTL() == -2) {
            ttl_st->set_lat_ttl(0);
        } else {
            ttl_st->set_lat_ttl(base::LatTTLConvert(column_index->GetLatTTL(), true));
        }
    }
    if (!column_index->GetTs().empty()) {
        // if no column_names, skip check
        if (!column_names.empty()) {
            auto it = column_names.find(column_index->GetTs());
            if (it == column_names.end()) {
                status->msg = "CREATE common: TS NAME " + column_index->GetTs() + " not exists";
                status->code = hybridse::common::kUnsupportSql;
                return false;
            }
        }
        index->set_ts_name(column_index->GetTs());
    }
    return true;
}

std::shared_ptr<hybridse::node::ConstNode> NodeAdapter::TransformDataType(const hybridse::node::ConstNode& node,
                                                                          openmldb::type::DataType column_type) {
    hybridse::node::DataType node_type = node.GetDataType();
    switch (column_type) {
        case openmldb::type::kBool:
            if (node_type == hybridse::node::kInt32) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetBool());
            } else if (node_type == hybridse::node::kBool) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case openmldb::type::kSmallInt:
            if (node_type == hybridse::node::kInt16) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            } else if (node_type == hybridse::node::kInt32) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetAsInt16());
            }
            break;
        case openmldb::type::kInt:
            if (node_type == hybridse::node::kInt16) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetAsInt32());
            } else if (node_type == hybridse::node::kInt32) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            } else if (node_type == hybridse::node::kInt64) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetAsInt32());
            }
            break;
        case openmldb::type::kBigInt:
            if (node_type == hybridse::node::kInt16 || node_type == hybridse::node::kInt32) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetAsInt64());
            } else if (node_type == hybridse::node::kInt64) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case openmldb::type::kFloat:
            if (node_type == hybridse::node::kDouble || node_type == hybridse::node::kInt32 ||
                node_type == hybridse::node::kInt16) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetAsFloat());
            } else if (node_type == hybridse::node::kFloat) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case openmldb::type::kDouble:
            if (node_type == hybridse::node::kFloat || node_type == hybridse::node::kInt32 ||
                node_type == hybridse::node::kInt16) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetAsDouble());
            } else if (node_type == hybridse::node::kDouble) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case openmldb::type::kDate:
            if (node_type == hybridse::node::kVarchar) {
                int32_t year;
                int32_t month;
                int32_t day;
                if (node.GetAsDate(&year, &month, &day)) {
                    uint32_t date = 0;
                    if (!openmldb::codec::RowBuilder::ConvertDate(year, month, day, &date)) {
                        break;
                    }
                    return std::make_shared<hybridse::node::ConstNode>(static_cast<int32_t>(date));
                }
                break;
            } else if (node_type == hybridse::node::kDate) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case openmldb::type::kTimestamp:
            if (node_type == hybridse::node::kInt16 || node_type == hybridse::node::kInt32 ||
                node_type == hybridse::node::kTimestamp) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetAsInt64());
            } else if (node_type == hybridse::node::kInt64) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case openmldb::type::kVarchar:
        case openmldb::type::kString:
            if (node_type == hybridse::node::kVarchar) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        default:
            return std::shared_ptr<hybridse::node::ConstNode>();
    }
    return std::shared_ptr<hybridse::node::ConstNode>();
}

std::string NodeAdapter::DataToString(const hybridse::node::ConstNode& node) {
    switch (node.GetDataType()) {
        case hybridse::node::kInt16:
        case hybridse::node::kInt32:
        case hybridse::node::kInt64:
        case hybridse::node::kFloat:
        case hybridse::node::kDouble:
        case hybridse::node::kVarchar:
            return node.GetAsString();
        case hybridse::node::kBool:
            return std::to_string(node.GetBool());
        case hybridse::node::kDate:
            return std::to_string(node.GetInt());
        case hybridse::node::kTimestamp:
            return std::to_string(node.GetLong());
        default:
            return "";
    }
}

std::shared_ptr<hybridse::node::ConstNode> NodeAdapter::StringToData(const std::string& str,
                                                                     openmldb::type::DataType data_type) {
    try {
        switch (data_type) {
            case type::kBool:
                return std::make_shared<hybridse::node::ConstNode>(boost::lexical_cast<bool>(str));
            case type::kSmallInt:
                return std::make_shared<hybridse::node::ConstNode>(boost::lexical_cast<int16_t>(str));
            case type::kInt:
            case type::kDate:
                return std::make_shared<hybridse::node::ConstNode>(boost::lexical_cast<int32_t>(str));
            case type::kBigInt:
            case type::kTimestamp:
                return std::make_shared<hybridse::node::ConstNode>(boost::lexical_cast<int64_t>(str));
            case type::kFloat:
                return std::make_shared<hybridse::node::ConstNode>(boost::lexical_cast<float>(str));
            case type::kDouble:
                return std::make_shared<hybridse::node::ConstNode>(boost::lexical_cast<double>(str));
            case type::kVarchar:
            case type::kString:
                return std::make_shared<hybridse::node::ConstNode>(str);
            default:
                return std::shared_ptr<hybridse::node::ConstNode>();
        }
    } catch (std::exception const& e) {
        return std::shared_ptr<hybridse::node::ConstNode>();
    }
    return std::shared_ptr<hybridse::node::ConstNode>();
}

hybridse::sdk::Status NodeAdapter::ParseExprNode(const hybridse::node::BinaryExpr* expr_node,
        const std::map<std::string, openmldb::type::DataType>& col_map,
        const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& indexs,
        std::vector<Condition>* condition_vec, std::vector<Condition>* parameter_vec) {
    auto op_type = expr_node->GetOp();
    switch (op_type) {
        case hybridse::node::FnOperator::kFnOpAnd: {
            if (op_type == hybridse::node::FnOperator::kFnOpAnd) {
                for (size_t idx = 0; idx < expr_node->GetChildNum(); idx++) {
                    auto node = dynamic_cast<const hybridse::node::BinaryExpr*>(expr_node->GetChild(idx));
                    if (node == nullptr) {
                        return {::hybridse::common::StatusCode::kCmdError, "parse expr node failed"};
                    }
                    auto status = ParseExprNode(node, col_map, indexs, condition_vec, parameter_vec);
                    if (!status.IsOK()) {
                        return status;
                    }
                }
            }
            break;
        }
        case hybridse::node::FnOperator::kFnOpEq:
        case hybridse::node::FnOperator::kFnOpLt:
        case hybridse::node::FnOperator::kFnOpLe:
        case hybridse::node::FnOperator::kFnOpGt:
        case hybridse::node::FnOperator::kFnOpGe: {
            if (expr_node->GetChild(0)->GetExprType() != hybridse::node::ExprType::kExprColumnRef) {
                return {::hybridse::common::StatusCode::kCmdError, "parse node failed"};
            }
            auto column_node = dynamic_cast<const hybridse::node::ColumnRefNode*>(expr_node->GetChild(0));
            const auto& col_name = column_node->GetColumnName();
            auto iter = col_map.find(col_name);
            if (iter == col_map.end()) {
                return {::hybridse::common::StatusCode::kCmdError, "col " + col_name + " does not exist"};
            }
            std::optional<std::string> value;
            if (expr_node->GetChild(1)->GetExprType() == hybridse::node::ExprType::kExprPrimary) {
                auto value_node = dynamic_cast<const hybridse::node::ConstNode*>(expr_node->GetChild(1));
                if (value_node->IsNull()) {
                    value = std::nullopt;
                } else if (iter->second == openmldb::type::kDate &&
                        value_node->GetDataType() == hybridse::node::kVarchar) {
                    int32_t year;
                    int32_t month;
                    int32_t day;
                    if (!value_node->GetAsDate(&year, &month, &day)) {
                        return {::hybridse::common::StatusCode::kCmdError, "invalid date value"};
                    }
                    uint32_t date = 0;
                    if (!openmldb::codec::RowBuilder::ConvertDate(year, month, day, &date)) {
                        return {::hybridse::common::StatusCode::kCmdError, "invalid date value"};
                    }
                    value = std::to_string(date);
                } else {
                    value = value_node->GetAsString();
                }
                condition_vec->emplace_back(col_name, op_type, value, iter->second);
            } else if (expr_node->GetChild(1)->GetExprType() == hybridse::node::ExprType::kExprParameter) {
                auto value_node = dynamic_cast<const hybridse::node::ParameterExpr*>(expr_node->GetChild(1));
                value = std::to_string(value_node->position());
                parameter_vec->emplace_back(col_name, op_type, value, iter->second);
            } else {
                return {::hybridse::common::StatusCode::kCmdError, "parse node failed"};
            }
            break;
        }
        default:
            return {::hybridse::common::StatusCode::kCmdError,
                "unsupport operator type " + hybridse::node::ExprOpTypeName(op_type)};
    }
    if (parameter_vec->empty()) {
        return CheckCondition(indexs, *condition_vec);
    } else if (condition_vec->empty()) {
        return CheckCondition(indexs, *parameter_vec);
    }
    std::vector<Condition> conditions(*condition_vec);
    conditions.insert(conditions.end(), parameter_vec->begin(), parameter_vec->end());
    return CheckCondition(indexs, conditions);
}

}  // namespace openmldb::sdk
