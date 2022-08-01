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

DECLARE_uint32(partition_num);

namespace openmldb::sdk {

using hybridse::plan::PlanAPI;

bool NodeAdapter::TransformToTableDef(::hybridse::node::CreatePlanNode* create_node, bool allow_empty_col_index,
                                      ::openmldb::nameserver::TableInfo* table, uint32_t default_replica_num,
                                      bool is_cluster_mode, hybridse::base::Status* status) {
    if (create_node == nullptr || table == nullptr || status == nullptr) return false;
    std::string table_name = create_node->GetTableName();
    const hybridse::node::NodePointVector& column_desc_list = create_node->GetColumnDescList();
    const hybridse::node::NodePointVector& table_option_list = create_node->GetTableOptionList();
    std::set<std::string> index_names;
    std::map<std::string, ::openmldb::common::ColumnDesc*> column_names;
    table->set_name(table_name);
    hybridse::node::NodePointVector distribution_list;

    hybridse::node::StorageMode storage_mode = hybridse::node::kMemory;
    // different default value for cluster and standalone mode
    int replica_num = 1;
    int partition_num = 1;
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
                    break;
                }
                case hybridse::node::kPartitionNum: {
                    partition_num =
                        dynamic_cast<hybridse::node::PartitionNumNode*>(table_option)->GetPartitionNum();
                    break;
                }
                case hybridse::node::kStorageMode: {
                    storage_mode = dynamic_cast<hybridse::node::StorageModeNode *>(table_option)->GetStorageMode();
                    break;
                }
                case hybridse::node::kDistributions: {
                    auto d_list = dynamic_cast<hybridse::node::DistributionsNode*>(table_option)->GetDistributionList();
                    if (d_list != nullptr) {
                        for (auto meta_ptr : d_list->GetList()) {
                            distribution_list.push_back(meta_ptr);
                        }
                    }
                    break;
                }
                default: {
                    LOG(WARNING) << "can not handle type " << NameOfSqlNodeType(table_option->GetType())
                                << " for table node";
                }
            }
        }
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

    table->set_format_version(1);
    table->set_storage_mode(static_cast<common::StorageMode>(storage_mode));
    bool has_generate_index = false;
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
                switch (column_def->GetColumnType()) {
                    case hybridse::node::kBool:
                        add_column_desc->set_data_type(openmldb::type::DataType::kBool);
                        break;
                    case hybridse::node::kInt16:
                        add_column_desc->set_data_type(openmldb::type::DataType::kSmallInt);
                        break;
                    case hybridse::node::kInt32:
                        add_column_desc->set_data_type(openmldb::type::DataType::kInt);
                        break;
                    case hybridse::node::kInt64:
                        add_column_desc->set_data_type(openmldb::type::DataType::kBigInt);
                        break;
                    case hybridse::node::kFloat:
                        add_column_desc->set_data_type(openmldb::type::DataType::kFloat);
                        break;
                    case hybridse::node::kDouble:
                        add_column_desc->set_data_type(openmldb::type::DataType::kDouble);
                        break;
                    case hybridse::node::kTimestamp:
                        add_column_desc->set_data_type(openmldb::type::DataType::kTimestamp);
                        break;
                    case hybridse::node::kVarchar:
                        add_column_desc->set_data_type(openmldb::type::DataType::kVarchar);
                        break;
                    case hybridse::node::kDate:
                        add_column_desc->set_data_type(openmldb::type::DataType::kDate);
                        break;
                    default: {
                        status->msg = "CREATE common: column type " +
                                      hybridse::node::DataTypeName(column_def->GetColumnType()) + " is not supported";
                        status->code = hybridse::common::kUnsupportSql;
                        return false;
                    }
                }
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
                    if (allow_empty_col_index && !has_generate_index && !column_index->GetTs().empty()) {
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
        if (replica_num != static_cast<int32_t>(distribution_list.size())) {
            status->msg =
                "CREATE common: "
                "replica_num should equal to partition meta size";
            status->code = hybridse::common::kUnsupportSql;
            return false;
        }
        ::openmldb::nameserver::TablePartition* table_partition = table->add_table_partition();
        table_partition->set_pid(0);
        std::vector<std::string> ep_vec;
        for (auto partition_meta : distribution_list) {
            switch (partition_meta->GetType()) {
                case hybridse::node::kPartitionMeta: {
                    auto* p_meta_node = dynamic_cast<hybridse::node::PartitionMetaNode*>(partition_meta);
                    const std::string& ep = p_meta_node->GetEndpoint();
                    if (std::find(ep_vec.begin(), ep_vec.end(), ep) != ep_vec.end()) {
                        status->msg =
                            "CREATE common: "
                            "partition meta endpoint duplicate";
                        status->code = hybridse::common::kUnsupportSql;
                        return false;
                    }
                    ep_vec.push_back(ep);
                    ::openmldb::nameserver::PartitionMeta* meta = table_partition->add_partition_meta();
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
                    status->msg = "can not support " + hybridse::node::NameOfSqlNodeType(partition_meta->GetType()) +
                                  " when CREATE TABLE 2";
                    status->code = hybridse::common::kUnsupportSql;
                    return false;
                }
            }
        }
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
            std::map<std::string, std::string>* condition_map, std::map<std::string, int>* parameter_map) {
    auto op_type = expr_node->GetOp();
    if (op_type == hybridse::node::FnOperator::kFnOpAnd) {
        for (size_t idx = 0; idx < expr_node->GetChildNum(); idx++) {
            auto node = dynamic_cast<const hybridse::node::BinaryExpr*>(expr_node->GetChild(idx));
            if (node == nullptr) {
                return {::hybridse::common::StatusCode::kCmdError, "parse expr node failed"};
            }
            auto status = ParseExprNode(node, condition_map, parameter_map);
            if (!status.IsOK()) {
                return status;
            }
        }
    } else if (op_type == hybridse::node::FnOperator::kFnOpEq) {
        if (expr_node->GetChild(0)->GetExprType() != hybridse::node::ExprType::kExprColumnRef) {
            return {::hybridse::common::StatusCode::kCmdError, "parse node failed"};
        }
        auto column_node = dynamic_cast<const hybridse::node::ColumnRefNode*>(expr_node->GetChild(0));
        if (expr_node->GetChild(1)->GetExprType() == hybridse::node::ExprType::kExprPrimary) {
            auto value_node = dynamic_cast<const hybridse::node::ConstNode*>(expr_node->GetChild(1));
            condition_map->emplace(column_node->GetColumnName(), value_node->GetAsString());
        } else if (expr_node->GetChild(1)->GetExprType() == hybridse::node::ExprType::kExprParameter) {
            auto value_node = dynamic_cast<const hybridse::node::ParameterExpr*>(expr_node->GetChild(1));
            parameter_map->emplace(column_node->GetColumnName(), value_node->position());
        } else {
            return {::hybridse::common::StatusCode::kCmdError, "parse node failed"};
        }
    } else {
        return {::hybridse::common::StatusCode::kCmdError,
            "unsupport operator type " + hybridse::node::ExprOpTypeName(op_type)};
    }
    return {};
}

}  // namespace openmldb::sdk
