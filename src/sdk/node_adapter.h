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

#ifndef SRC_SDK_NODE_ADAPTER_H_
#define SRC_SDK_NODE_ADAPTER_H_

#include <map>
#include <memory>
#include <string>

#include "node/node_manager.h"
#include "proto/name_server.pb.h"
#include "proto/type.pb.h"

namespace openmldb {
namespace sdk {

class NodeAdapter {
 public:
    static bool TransformToTableDef(::hybridse::node::CreatePlanNode* create_node,
                                    ::openmldb::nameserver::TableInfo* table, uint32_t default_replica_num,
                                    bool is_cluster_mode, hybridse::base::Status* status);

    static bool TransformToColumnKey(hybridse::node::ColumnIndexNode* column_index,
                                     const std::map<std::string, ::openmldb::common::ColumnDesc*>& column_names,
                                     common::ColumnKey* index, hybridse::base::Status* status);

    static std::shared_ptr<hybridse::node::ConstNode> TransformDataType(const hybridse::node::ConstNode& node,
                                                                        openmldb::type::DataType column_type);

    static std::string DataToString(const hybridse::node::ConstNode& node);

    static std::shared_ptr<hybridse::node::ConstNode> StringToData(const std::string& str,
                                                                   openmldb::type::DataType data_type);

    static hybridse::sdk::Status ParseExprNode(const hybridse::node::BinaryExpr* expr_node,
            const std::map<std::string, openmldb::type::DataType>& col_map,
            std::map<std::string, std::string>* condition_map, std::map<std::string, int>* parameter_map);
};

}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_NODE_ADAPTER_H_
