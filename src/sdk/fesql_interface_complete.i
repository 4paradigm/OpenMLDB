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

%module fesql_interface
%include fesql_interface_core.i

%shared_ptr(fesql::sdk::DBMSSdk);
%shared_ptr(fesql::sdk::TabletSdk);
%shared_ptr(fesql::sdk::Schema);
%shared_ptr(fesql::sdk::Table);
%shared_ptr(fesql::sdk::TableSet);
%shared_ptr(fesql::sdk::ResultSet);
%shared_ptr(fesql::sdk::Date);
%shared_ptr(fesql::sdk::RequestRow);

%{
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "sdk/base.h"
#include "base/iterator.h"
#include "sdk/request_row.h"
#include "sdk/result_set.h"
#include "sdk/tablet_sdk.h"
#include "sdk/dbms_sdk.h"
#include "vm/catalog.h"
#include "vm/engine.h"
#include "vm/jit_wrapper.h"
#include "vm/physical_op.h"
#include "vm/simple_catalog.h"

using namespace fesql;
using fesql::sdk::Schema;
using fesql::sdk::ResultSet;
using fesql::sdk::Table;
using fesql::sdk::TableSet;
using fesql::sdk::RequestRow;
using fesql::sdk::DBMSSdk;
using fesql::sdk::TabletSdk;
using namespace fesql::node;
using fesql::vm::SQLContext;
using fesql::vm::Catalog;
using fesql::vm::PhysicalOpNode;
using fesql::vm::PhysicalSimpleProjectNode;
using fesql::vm::RowView;
using fesql::vm::FnInfo;
using fesql::vm::Sort;
using fesql::vm::Range;
using fesql::vm::ConditionFilter;
using fesql::vm::ColumnProjects;
using fesql::vm::Key;
using fesql::vm::WindowOp;
using fesql::vm::EngineMode;
using fesql::base::Iterator;
using fesql::base::ConstIterator;
using fesql::codec::RowIterator;
using fesql::codec::Row;
using fesql::vm::SchemasContext;
using fesql::vm::SchemaSource;
using fesql::node::PlanType;
using fesql::sdk::ExplainInfo;
%}

%include "sdk/base.h"
%include "sdk/result_set.h"
%include "sdk/request_row.h"
%include "sdk/dbms_sdk.h"
%include "sdk/tablet_sdk.h"
