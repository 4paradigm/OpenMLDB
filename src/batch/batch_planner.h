/*
 * batch_planner.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
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

#ifndef SRC_BATCH_BATCH_PLANNER_H_
#define SRC_BATCH_BATCH_PLANNER_H_

#include <memory>
#include <string>
#include "batch/batch_catalog.h"
#include "proto/plan.pb.h"

namespace fesql {
namespace batch {

// convert sql to dag
class BatchPlanner {
 public:
    BatchPlanner(const std::shared_ptr<BatchCatalog>& catalog,
                 const std::string& db, const std::string& sql);

    ~BatchPlanner();

    bool MakePlan(GraphDesc* graph);

 private:
    std::shared_ptr<BatchCatalog> catalog_;
    std::string db_;
    std::string sql_;
};

}  // namespace batch
}  // namespace fesql

#endif  // SRC_BATCH_BATCH_PLANNER_H_
