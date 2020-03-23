%module interclient
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"
%include "std_map.i"
%include "std_set.i"
%{
#include "client/client.h"
%}
namespace std {
   %template(VectorString) vector<string>;
   %template(MapStringString) map<string, string>;
   %template(SetString) set<string>;
}

%include "client/client.h"
namespace std {
        %template(VectorReadFilter) vector<ReadFilter>;
        %template(VectorReadOption) vector<ReadOption>;
}
%include "base/slice.h"
%include "base/status.h"
%include "client/ns_client.h"
%include "base/schema_codec.h"
