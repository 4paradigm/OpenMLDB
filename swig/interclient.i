%module interclient
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"
%include "std_map.i"
%include "std_set.i"
%{
#include "client.h"
%}
namespace std {
   %template(VectorString) vector<string>;
   %template(MapStringString) map<string, string>;
   %template(SetString) set<string>;
}

%include "client.h"
namespace std {
        %template(VectorReadFilter) vector<ReadFilter>;
        %template(MapStringColumn) map<string, GetColumn>;
}
%include "client/ns_client.h"
%include "base/schema_codec.h"
