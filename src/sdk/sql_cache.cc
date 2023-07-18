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

#include "sdk/sql_cache.h"
namespace openmldb {
namespace sdk {

bool RouterSQLCache::IsCompatibleCache(const std::shared_ptr<::hybridse::sdk::Schema>& other_parameter_schema) const {
    if (!parameter_schema_ && !other_parameter_schema) {
        return true;
    }
    if (!parameter_schema_ || !other_parameter_schema) {
        return false;
    }
    if (parameter_schema_->GetColumnCnt() != other_parameter_schema->GetColumnCnt()) {
        return false;
    }
    for (int i = 0; i < parameter_schema_->GetColumnCnt(); i++) {
        if (parameter_schema_->GetColumnType(i) != other_parameter_schema->GetColumnType(i)) {
            return false;
        }
    }
    return true;
}

}  // namespace sdk
}  // namespace openmldb
