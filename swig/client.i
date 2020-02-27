%module client
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"
%{
#include "client.h"
%}

namespace std {
   %template(StringVector) vector<string>;
}

%include "client/ns_client.h"
%include "client.h"
