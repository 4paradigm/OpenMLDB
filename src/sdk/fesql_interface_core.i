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
%include stdint.i
%include std_vector.i

namespace std {
    %template(StringVector) vector<string>;
}

%shared_ptr(fesql::vm::Catalog);
%shared_ptr(fesql::vm::SimpleCatalog);
%shared_ptr(fesql::vm::CompileInfo);

%typemap(jni) fesql::vm::RawPtrHandle "jlong"
%typemap(jtype) fesql::vm::RawPtrHandle "long"
%typemap(jstype) fesql::vm::RawPtrHandle "long"
%typemap(javain) fesql::vm::RawPtrHandle "$javainput"
%typemap(javaout) fesql::vm::RawPtrHandle "{ return $jnicall; }"
%typemap(in) fesql::vm::RawPtrHandle %{ $1 = reinterpret_cast<fesql::vm::RawPtrHandle>($input); %}


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
%protobuf_repeated_typedef(fesql::codec::Schema, com._4paradigm.fesql.type.TypeOuterClass.ColumnDef);

// Enable direct buffer interfaces
%include "swig_library/java/buffer.i"
%as_direct_buffer(fesql::base::RawBuffer);

// Enable common numeric types
%include "swig_library/java/numerics.i"
#endif

%{
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "base/iterator.h"
#include "vm/catalog.h"
#include "vm/engine.h"
#include "vm/jit_wrapper.h"
#include "vm/physical_op.h"
#include "vm/simple_catalog.h"

using namespace fesql;
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
using fesql::codec::WindowIterator;
using fesql::node::DataType;
%}

%rename(BaseStatus) fesql::base::Status;
%ignore MakeExprWithTable; // TODO: avoid return object with share pointer
%ignore WindowIterator;
%ignore fesql::vm::SchemasContext;
%ignore fesql::vm::RowHandler;
%ignore fesql::vm::TableHandler;
%ignore fesql::vm::PartitionHandler;
%ignore fesql::vm::ErrorRowHandler;
%ignore fesql::vm::ErrorTableHandler;
%ignore fesql::vm::RequestUnionTableHandler;
%ignore fesql::vm::SimpleCatalogTableHandler;
%ignore fesql::vm::DataHandlerList;
%ignore fesql::vm::DataHandlerVector;
%ignore fesql::vm::DataHandlerRepeater;
%ignore fesql::vm::LocalTabletTableHandler;
%ignore fesql::vm::AysncRowHandler;
%ignore DataTypeName; // TODO: Geneerate duplicated class
%ignore fesql::vm::FeSQLJITWrapper::AddModule;

// Ignore the unique_ptr functions
%ignore fesql::vm::MemTableHandler::GetWindowIterator;
%ignore fesql::vm::MemTableHandler::GetIterator;
%ignore fesql::vm::MemTimeTableHandler::GetWindowIterator;
%ignore fesql::vm::MemTimeTableHandler::GetIterator;
%ignore fesql::vm::MemSegmentHandler::GetWindowIterator;
%ignore fesql::vm::MemSegmentHandler::GetIterator;
%ignore fesql::vm::MemPartitionHandler::GetWindowIterator;
%ignore fesql::vm::MemPartitionHandler::GetIterator;
%ignore fesql::vm::MemWindowIterator::GetValue;
%ignore fesql::vm::RequestUnionTableHandler::GetWindowIterator;
%ignore fesql::vm::RequestUnionTableHandler::GetIterator;
%ignore fesql::vm::MemCatalog;
%ignore fesql::vm::MemCatalog::~MemCatalog;
%ignore fesql::vm::AscComparor::operator();
%ignore fesql::vm::DescComparor::operator();

%include "base/fe_status.h"
%include "codec/row.h"
%include "codec/fe_row_codec.h"
%include "node/node_enum.h"
%include "node/plan_node.h"
%include "node/sql_node.h"
%include "vm/catalog.h"
%include "vm/simple_catalog.h"
%include "vm/schemas_context.h"
%include "vm/engine.h"
%include "vm/physical_op.h"
%include "vm/jit_wrapper.h"
%include "vm/core_api.h"
%include "vm/mem_catalog.h"
