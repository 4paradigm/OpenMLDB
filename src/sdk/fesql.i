%module fesql

%include std_string.i
%include std_shared_ptr.i
%include stl.i
namespace std {
    %template(StringVector) vector<string>;
}

%shared_ptr(fesql::sdk::DBMSSdk);
%shared_ptr(fesql::sdk::TabletSdk);
%shared_ptr(fesql::sdk::Schema);
%shared_ptr(fesql::sdk::Table);
%shared_ptr(fesql::sdk::TableSet);
%shared_ptr(fesql::sdk::ResultSet);

%{
#include "sdk/base.h"
#include "sdk/result_set.h"
#include "sdk/dbms_sdk.h"
#include "sdk/tablet_sdk.h"

using namespace fesql;
using fesql::sdk::Schema;
using fesql::sdk::ResultSet;
using fesql::sdk::Table;
using fesql::sdk::TableSet;
using fesql::sdk::DBMSSdk;
using fesql::sdk::TabletSdk;
%}

%include "sdk/result_set.h"
%include "sdk/base.h"
%include "sdk/dbms_sdk.h"
%include "sdk/tablet_sdk.h" 
