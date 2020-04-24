%module fesql_interface

// Enable public interfaces for Java
#ifdef SWIGJAVA
#define SWIG_SHARED_PTR_TYPEMAPS(CONST, TYPE...) \
	SWIG_SHARED_PTR_TYPEMAPS_IMPLEMENTATION(public, public, CONST, TYPE)
SWIG_JAVABODY_TYPEWRAPPER(public, public, public, SWIGTYPE)
SWIG_JAVABODY_PROXY(public, public, SWIGTYPE)
#endif


// Enable string and shared pointers
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
%shared_ptr(fesql::vm::Catalog);
%shared_ptr(fesql::vm::SimpleCatalog);
%shared_ptr(fesql::vm::CompileInfo);

%typemap(jni) fesql::vm::RawFunctionPtr "jlong"
%typemap(jtype) fesql::vm::RawFunctionPtr "long"
%typemap(jstype) fesql::vm::RawFunctionPtr "long"
%typemap(javain) fesql::vm::RawFunctionPtr "$javainput"
%typemap(javaout) fesql::vm::RawFunctionPtr "{ return $jnicall; }"
%typemap(in) fesql::vm::RawFunctionPtr %{ $1 = reinterpret_cast<fesql::vm::RawFunctionPtr>($input); %}


// Fix for Java shared_ptr unref
// %feature("unref") fesql::vm::Catalog "delete $this;"


#ifdef SWIGJAVA
// Enable namespace feature for Java
%nspace;

// Fix for wrapper class imports
%typemap(javaimports) SWIGTYPE "import com._4paradigm.*;"

// Enable protobuf interfaces
%include "swig_library/java/protobuf.i"
%protobuf_enum(fesql::type::Type, com._4paradigm.fesql.type.TypeOuterClass.Type);
%protobuf(fesql::type::Database, com._4paradigm.fesql.type.TypeOuterClass.Database);
%protobuf(fesql::type::TableDef, com._4paradigm.fesql.type.TypeOuterClass.TableDef);
%protobuf_repeated_typedef(fesql::vm::Schema, com._4paradigm.fesql.type.TypeOuterClass.ColumnDef);

// Enable direct buffer interfaces
%include "swig_library/java/buffer.i"
%as_direct_buffer(fesql::base::RawBuffer);

// Enable common numeric types
%include "swig_library/java/numerics.i"
#endif


%{
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "sdk/base.h"
#include "sdk/dbms_sdk.h"
#include "sdk/result_set.h"
#include "sdk/tablet_sdk.h"
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
using fesql::sdk::DBMSSdk;
using fesql::sdk::TabletSdk;

using namespace fesql::node;
using fesql::vm::SQLContext;
using fesql::vm::Catalog;
using fesql::vm::PhysicalOpNode;
using fesql::vm::RowView;
using fesql::vm::FnInfo;
using fesql::codec::RowIterator;
using fesql::codec::IteratorV;
using fesql::codec::Row;
using fesql::node::PlanType;
%}


%rename(BaseStatus) fesql::base::Status;
%ignore MakeExprWithTable; // TODO: avoid return object with share pointer
%ignore WindowIterator;
%ignore fesql::vm::SchemasContext;
%ignore fesql::vm::RowSchemaInfo;
%ignore fesql::vm::RowHandler;
%ignore fesql::vm::TableHandler;
%ignore fesql::vm::PartitionHandler;
%ignore fesql::vm::SimpleCatalogTableHandler;
%ignore DataTypeName; // TODO: Geneerate duplicated class
%ignore fesql::vm::RequestRunSession::RunRequestPlan;
%ignore fesql::vm::FeSQLJITWrapper::AddModule;

%include "base/slice.h"
%include "base/status.h"
%include "codec/row.h"
%include "codec/row_codec.h"
%include "sdk/result_set.h"
%include "sdk/base.h"
%include "sdk/dbms_sdk.h"
%include "sdk/tablet_sdk.h" 
%include "node/plan_node.h"
%include "node/sql_node.h"
%include "vm/catalog.h"
%include "vm/simple_catalog.h"
%include "vm/engine.h"
%include "vm/physical_op.h"
%include "vm/jit_wrapper.h"
%include "vm/core_api.h"
