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
   %template(StringVector) vector<string>;
   %template(map_string_string) map<string, string>;
   %template(set_strig) set<string>;
}

%include "client.h"

namespace std {
        %template(ReadFilterVector) vector<ReadFilter>;
}
%include "client/ns_client.h"
%include "base/schema_codec.h"
