%module interclient_tools
%begin %{
#define SWIG_PYTHON_STRICT_BYTE_CHAR
%}
%include "stdint.i"
%include "cstring.i"
%cstring_output_allocate_size(char** packet, int64_t* sz, free(*$1));
%{
#include "client/client_type.h"
#include "client/interclient_tools.h"
%}
%include "client/client_type.h"
%include "client/interclient_tools.h"
