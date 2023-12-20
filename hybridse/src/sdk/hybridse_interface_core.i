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

%module hybridse_interface
#pragma SWIG nowarn=315, 401, 503, 516, 822
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

%shared_ptr(hybridse::vm::Catalog);
%shared_ptr(hybridse::vm::SimpleCatalog);
%shared_ptr(hybridse::vm::CompileInfo);
%shared_ptr(hybridse::vm::SqlCompileInfo);
%shared_ptr(hybridse::vm::IndexHintHandler);

%typemap(jni) hybridse::vm::RawPtrHandle "jlong"
%typemap(jtype) hybridse::vm::RawPtrHandle "long"
%typemap(jstype) hybridse::vm::RawPtrHandle "long"
%typemap(javain) hybridse::vm::RawPtrHandle "$javainput"
%typemap(javaout) hybridse::vm::RawPtrHandle "{ return $jnicall; }"
%typemap(in) hybridse::vm::RawPtrHandle %{ $1 = reinterpret_cast<hybridse::vm::RawPtrHandle>($input); %}

#ifdef SWIGJAVA
// typemap from https://github.com/swig/swig/blob/master/Lib/java/various.i
%typemap(jni) hybridse::vm::ByteArrayPtr "jbyteArray"
%typemap(jtype) hybridse::vm::ByteArrayPtr "byte[]"
%typemap(jstype) hybridse::vm::ByteArrayPtr "byte[]"
%typemap(in) hybridse::vm::ByteArrayPtr {
  $1 = (hybridse::vm::ByteArrayPtr) JCALL2(GetByteArrayElements, jenv, $input, 0);
}

%typemap(argout) hybridse::vm::ByteArrayPtr {
  JCALL3(ReleaseByteArrayElements, jenv, $input, (jbyte *) $1, 0);
}

%typemap(javain) hybridse::vm::ByteArrayPtr "$javainput"
%typemap(javaout) hybridse::vm::ByteArrayPtr "{ return $jnicall; }"

/* Prevent default freearg typemap from being used */
%typemap(freearg) hybridse::vm::ByteArrayPtr ""

%typemap(jni) hybridse::vm::NIOBUFFER "jobject"
%typemap(jtype) hybridse::vm::NIOBUFFER "java.nio.ByteBuffer"
%typemap(jstype) hybridse::vm::NIOBUFFER "java.nio.ByteBuffer"
%typemap(javain,
  pre="  assert $javainput.isDirect() : \"Buffer must be allocated direct.\";") hybridse::vm::NIOBUFFER "$javainput"
%typemap(javaout) hybridse::vm::NIOBUFFER {
  return $jnicall;
}
%typemap(in) hybridse::vm::NIOBUFFER {
  $1 = (unsigned char *) JCALL1(GetDirectBufferAddress, jenv, $input);
  if ($1 == NULL) {
    SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get address of a java.nio.ByteBuffer direct byte buffer. Buffer must be a direct buffer and not a non-direct buffer.");
  }
}
%typemap(memberin) hybridse::vm::NIOBUFFER {
  if ($input) {
    $1 = $input;
  } else {
    $1 = 0;
  }
}
%typemap(freearg) hybridse::vm::NIOBUFFER ""
#endif

// Fix for Java shared_ptr unref
// %feature("unref") hybridse::vm::Catalog "delete $this;"

#ifdef SWIGJAVA
// Enable namespace feature for Java
%nspace;

// Fix for wrapper class imports
%typemap(javaimports) SWIGTYPE "import com._4paradigm.*;"

// Enable protobuf interfaces
%include "swig_library/java/protobuf.i"
%protobuf_enum(hybridse::type::Type, com._4paradigm.hybridse.type.TypeOuterClass.Type);
%protobuf(hybridse::type::Database, com._4paradigm.hybridse.type.TypeOuterClass.Database);
%protobuf(hybridse::type::TableDef, com._4paradigm.hybridse.type.TypeOuterClass.TableDef);
%protobuf_repeated_typedef(hybridse::codec::Schema, com._4paradigm.hybridse.type.TypeOuterClass.ColumnDef);

// Enable direct buffer interfaces
%include "swig_library/java/buffer.i"
%as_direct_buffer(hybridse::base::RawBuffer);

// Enable common numeric types
%include "swig_library/java/numerics.i"
#endif

%{
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "base/iterator.h"
#include "vm/catalog.h"
#include "vm/engine.h"
#include "vm/engine_context.h"
#include "vm/sql_compiler.h"
#include "vm/jit_wrapper.h"
#include "vm/physical_op.h"
#include "vm/simple_catalog.h"

using namespace hybridse;
using namespace hybridse::node;
using hybridse::vm::Catalog;
using hybridse::vm::PhysicalOpNode;
using hybridse::vm::PhysicalSimpleProjectNode;
using hybridse::codec::RowView;
using hybridse::vm::FnInfo;
using hybridse::vm::Sort;
using hybridse::vm::Range;
using hybridse::vm::ConditionFilter;
using hybridse::vm::ColumnProjects;
using hybridse::vm::Key;
using hybridse::vm::WindowOp;
using hybridse::vm::EngineMode;
using hybridse::vm::EngineOptions;
using hybridse::vm::IndexHintHandler;
using hybridse::base::Iterator;
using hybridse::base::ConstIterator;
using hybridse::base::Trace;
using hybridse::codec::RowIterator;
using hybridse::codec::Row;
using hybridse::vm::SchemasContext;
using hybridse::vm::SchemaSource;
using hybridse::node::PlanType;
using hybridse::codec::WindowIterator;
using hybridse::node::DataType;
%}

%rename(BaseStatus) hybridse::base::Status;
%ignore MakeExprWithTable; // TODO: avoid return object with share pointer
%ignore WindowIterator;
%ignore hybridse::vm::SchemasContext;
%ignore hybridse::vm::RowHandler;
%ignore hybridse::vm::TableHandler;
%ignore hybridse::vm::PartitionHandler;
%ignore hybridse::vm::ErrorRowHandler;
%ignore hybridse::vm::ErrorTableHandler;
%ignore hybridse::vm::RequestUnionTableHandler;
%ignore hybridse::vm::SimpleCatalogTableHandler;
%ignore hybridse::vm::DataHandlerList;
%ignore hybridse::vm::DataHandlerVector;
%ignore hybridse::vm::DataHandlerRepeater;
%ignore hybridse::vm::LocalTabletTableHandler;
%ignore hybridse::vm::AysncRowHandler;
%ignore DataTypeName; // TODO: Generate duplicated class
%ignore hybridse::vm::HybridSeJitWrapper::AddModule;

// Ignore the unique_ptr functions
%ignore hybridse::vm::MemTableHandler::GetWindowIterator;
%ignore hybridse::vm::MemTableHandler::GetIterator;
%ignore hybridse::vm::MemTimeTableHandler::GetWindowIterator;
%ignore hybridse::vm::MemTimeTableHandler::GetIterator;
%ignore hybridse::vm::MemSegmentHandler::GetWindowIterator;
%ignore hybridse::vm::MemSegmentHandler::GetIterator;
%ignore hybridse::vm::MemPartitionHandler::GetWindowIterator;
%ignore hybridse::vm::MemPartitionHandler::GetIterator;
%ignore hybridse::vm::MemWindowIterator::GetValue;
%ignore hybridse::vm::RequestUnionTableHandler::GetWindowIterator;
%ignore hybridse::vm::RequestUnionTableHandler::GetIterator;
%ignore hybridse::vm::MemCatalog;
%ignore hybridse::vm::MemCatalog::~MemCatalog;
%ignore hybridse::vm::AscComparor::operator();
%ignore hybridse::vm::DescComparor::operator();

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
%include "vm/engine_context.h"
%include "vm/sql_compiler.h"
%include "vm/physical_op.h"
%include "vm/jit_wrapper.h"
%include "vm/core_api.h"
%include "vm/mem_catalog.h"

%template(VectorDataType) std::vector<hybridse::node::DataType>;
