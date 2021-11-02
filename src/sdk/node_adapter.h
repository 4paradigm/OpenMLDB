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
#include <string>
#include "node/node_manager.h"
#include "proto/name_server.pb.h"

namespace openmldb {
namespace sdk {

class NodeAdapter {
 public:
    static bool TransformToTableDef(::hybridse::node::CreatePlanNode* create_node, bool allow_empty_col_index,
            ::openmldb::nameserver::TableInfo* table, hybridse::base::Status* status);

    static bool TransformToColumnKey(hybridse::node::ColumnIndexNode* column_index,
            const std::map<std::string, ::openmldb::common::ColumnDesc*>& column_names,
            common::ColumnKey* index, hybridse::base::Status* status);
};

}  // namespace sdk
}  // namespace openmldb

#endif  // SRC_SDK_NODE_ADAPTER_H_
