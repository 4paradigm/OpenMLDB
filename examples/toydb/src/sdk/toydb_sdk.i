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

%module toydb_sdk
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

%shared_ptr(fesql::sdk::DBMSSdk);
%shared_ptr(fesql::sdk::TabletSdk);
%shared_ptr(fesql::sdk::Schema);
%shared_ptr(fesql::sdk::Table);
%shared_ptr(fesql::sdk::TableSet);
%shared_ptr(fesql::sdk::ResultSet);
%shared_ptr(fesql::sdk::Date);
%shared_ptr(fesql::sdk::RequestRow);

%{
#include "sdk/base.h"
#include "sdk/request_row.h"
#include "sdk/result_set.h"
#include "sdk/tablet_sdk.h"
#include "sdk/dbms_sdk.h"

using namespace fesql;
using fesql::sdk::Schema;
using fesql::sdk::ResultSet;
using fesql::sdk::Table;
using fesql::sdk::TableSet;
using fesql::sdk::RequestRow;
using fesql::sdk::DBMSSdk;
using fesql::sdk::TabletSdk;
using fesql::sdk::ExplainInfo;
%}

%include "sdk/base.h"
%include "sdk/result_set.h"
%include "sdk/request_row.h"
%include "sdk/dbms_sdk.h"
%include "sdk/tablet_sdk.h"
