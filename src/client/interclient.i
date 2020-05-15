%module interclient
%begin %{
#define SWIG_PYTHON_STRICT_BYTE_CHAR
%}
%include "std_string.i"
%include "std_vector.i"
%include "std_array.i"
%include "stdint.i"
%include "std_map.i"
%include "std_set.i"
%include "cstring.i"
%cstring_output_allocate_size(char** packet, int64_t* sz, free(*$1));
%{
#include "client/client.h"
%}
namespace std {
   %template(VectorString) vector<string>;
   %template(MapStringString) map<string, string>;
   %template(SetString) set<string>;
   %template(MapSlice) map<string, rtidb::base::Slice>;
}

%include "client/client.h"
namespace std {
        %template(VectorReadFilter) vector<ReadFilter>;
        %template(VectorReadOption) vector<ReadOption>;
}
%include "base/slice.h"
%include "base/status.h"
%include "client/ns_client.h"
%include "codec/schema_codec.h"
