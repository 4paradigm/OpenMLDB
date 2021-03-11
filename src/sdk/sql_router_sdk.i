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

%module sql_router_sdk
%include "std_unique_ptr.i"
%include std_string.i
%include std_shared_ptr.i
%include stl.i
%include stdint.i
%include std_vector.i
#ifdef SWIGJAVA
%include various.i
%apply char *BYTE { char *string_buffer_var_name };
#endif

%shared_ptr(fesql::sdk::ResultSet);
%shared_ptr(fesql::sdk::Schema);
%shared_ptr(rtidb::sdk::SQLRouter);
%shared_ptr(rtidb::sdk::SQLRequestRow);
%shared_ptr(rtidb::sdk::SQLRequestRowBatch);
%shared_ptr(rtidb::sdk::ColumnIndicesSet);
%shared_ptr(rtidb::sdk::SQLInsertRow);
%shared_ptr(rtidb::sdk::SQLInsertRows);
%shared_ptr(rtidb::sdk::ExplainInfo);
%shared_ptr(fesql::sdk::ProcedureInfo);
%shared_ptr(rtidb::sdk::QueryFuture);
%shared_ptr(rtidb::sdk::TableReader);
%template(VectorUint32) std::vector<uint32_t>;
%template(VectorString) std::vector<std::string>;

%{
#include "sdk/sql_router.h"
#include "sdk/result_set.h"
#include "sdk/base.h"
#include "sdk/sql_request_row.h"
#include "sdk/sql_insert_row.h"
#include "sdk/table_reader.h"

using fesql::sdk::Schema;
using fesql::sdk::ResultSet;
using rtidb::sdk::SQLRouter;
using rtidb::sdk::SQLRouterOptions;
using rtidb::sdk::SQLRequestRow;
using rtidb::sdk::SQLRequestRowBatch;
using rtidb::sdk::ColumnIndicesSet;
using rtidb::sdk::SQLInsertRow;
using rtidb::sdk::SQLInsertRows;
using rtidb::sdk::ExplainInfo;
using fesql::sdk::ProcedureInfo;
using rtidb::sdk::QueryFuture;
using rtidb::sdk::TableReader;
%}

%include "sdk/sql_router.h"
%include "sdk/base.h"
%include "sdk/result_set.h"
%include "sdk/sql_request_row.h"
%include "sdk/sql_insert_row.h"
%include "sdk/table_reader.h"
