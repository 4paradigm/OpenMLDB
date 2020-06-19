%module interclient
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"
%include "std_map.i"
%include "std_set.i"
%include "std_shared_ptr.i"
%shared_ptr(rtidb::client::BsClient)
%include "cstring.i"
%cstring_output_allocate_size(char** packet, int64_t* sz, free(*$1));
%{
#include "client/client_type.h"
#include "client/client.h"
%}
namespace std {
   %template(VectorString) vector<string>;
   %template(MapStringString) map<string, string>;
   %template(SetString) set<string>;
   %template(VectorInt32) vector<int32_t>;
}
%include "client/client_type.h"
%include "client/client.h"
namespace std {
   %template(VectorReadFilter) vector<ReadFilter>;
   %template(VectorReadOption) vector<ReadOption>;
   %template(VectorInt64) vector<long long>;
}
%include "base/slice.h"
%include "base/status.h"
%include "codec/schema_codec.h"
