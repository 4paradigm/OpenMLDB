/*
 * Copyright 2021 4Paradigm
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

#ifndef SRC_TESTING_TEST_BASE_H_
#define SRC_TESTING_TEST_BASE_H_

#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "case/sql_case.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "plan/planner.h"
#include "vm/catalog.h"
#include "vm/engine.h"
#include "vm/simple_catalog.h"

namespace hybridse {
namespace vm {
using hybridse::base::Status;
using hybridse::codec::Row;
using hybridse::common::kTestEngineError;
using hybridse::sqlcase::SqlCase;
void BuildTableDef(::hybridse::type::TableDef& table);    // NOLINT
void BuildTableA(::hybridse::type::TableDef& table);      // NOLINT
void BuildTableT2Def(::hybridse::type::TableDef& table);  // NOLINT
void BuildBuf(int8_t** buf, uint32_t* size);
void BuildT2Buf(int8_t** buf, uint32_t* size);
void BuildRows(::hybridse::type::TableDef& table,    // NOLINT
               std::vector<Row>& rows);              // NOLINT
void BuildT2Rows(::hybridse::type::TableDef& table,  // NOLINT
                 std::vector<Row>& rows);            // NOLINT
void ExtractExprListFromSimpleSql(::hybridse::node::NodeManager* nm,
                                  const std::string& sql,
                                  node::ExprListNode* output);
void ExtractExprFromSimpleSql(::hybridse::node::NodeManager* nm,
                              const std::string& sql, node::ExprNode** output);
bool AddTable(hybridse::type::Database& db,  // NOLINT
              const hybridse::type::TableDef& table_def);
std::shared_ptr<SimpleCatalog> BuildSimpleCatalog(
    const hybridse::type::Database& database);
std::shared_ptr<SimpleCatalog> BuildSimpleCatalogIndexUnsupport(
    const hybridse::type::Database& database);

std::shared_ptr<SimpleCatalog> BuildSimpleCatalog();
bool InitSimpleCataLogFromSqlCase(SqlCase& sql_case,  // NOLINT
                                  std::shared_ptr<SimpleCatalog> catalog);

void PrintSchema(std::ostringstream& ss, const Schema& schema);
void PrintSchema(const Schema& schema);
}  // namespace vm
}  // namespace hybridse

#endif  // SRC_TESTING_TEST_BASE_H_
