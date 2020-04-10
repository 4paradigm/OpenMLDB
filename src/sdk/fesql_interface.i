%module fesql_interface

#ifdef SWIGJAVA
#define SWIG_SHARED_PTR_TYPEMAPS(CONST, TYPE...) SWIG_SHARED_PTR_TYPEMAPS_IMPLEMENTATION(public, public, CONST, TYPE)
#endif

SWIG_JAVABODY_TYPEWRAPPER(public, public, public, SWIGTYPE)
SWIG_JAVABODY_PROXY(public, public, SWIGTYPE)



%include std_string.i
%include std_shared_ptr.i
%include stl.i
namespace std {
    %template(StringVector) vector<string>;
}

%typemap(javaimports) SWIGTYPE "import com._4paradigm.*;"

%shared_ptr(fesql::sdk::DBMSSdk);
%shared_ptr(fesql::sdk::TabletSdk);
%shared_ptr(fesql::sdk::Schema);
%shared_ptr(fesql::sdk::Table);
%shared_ptr(fesql::sdk::TableSet);
%shared_ptr(fesql::sdk::ResultSet);

%nspace;


%{
#include "sdk/base.h"
#include "sdk/result_set.h"
#include "sdk/dbms_sdk.h"
#include "sdk/tablet_sdk.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "vm/engine.h"
#include "vm/catalog.h"

using namespace fesql;
using fesql::sdk::Schema;
using fesql::sdk::ResultSet;
using fesql::sdk::Table;
using fesql::sdk::TableSet;
using fesql::sdk::DBMSSdk;
using fesql::sdk::TabletSdk;

using namespace fesql::node;
//using namespace fesql::vm;
using fesql::vm::SQLContext;
using fesql::vm::Catalog;
using fesql::vm::PhysicalOpNode;
using fesql::codec::SliceIterator;
using fesql::codec::IteratorV;
using fesql::base::Slice;
using fesql::node::PlanType;
%}


%typemap(javaimports) SWIGTYPE "import com._4paradigm.*;"

%ignore MakeExprWithTable; // TODO: avoid return object with share pointer
%ignore WindowIterator;

%ignore fesql::vm::RowHandler;
%ignore fesql::vm::TableHandler;
%ignore fesql::vm::PartitionHandler;
%ignore DataTypeName; // TODO: Geneerate duplicated class
%ignore fesql::vm::RequestRunSession::RunRequestPlan;

%include "sdk/result_set.h"
%include "sdk/base.h"
%include "sdk/dbms_sdk.h"
%include "sdk/tablet_sdk.h" 
%include "node/plan_node.h"
%include "node/sql_node.h"
%include "vm/catalog.h"
%include "vm/engine.h"
