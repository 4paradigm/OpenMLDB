%module fesql

%include "std_string.i"
%include "std_unique_ptr.i"

%{
#include "sdk/base.h"
#include "sdk/result_set.h"
#include "sdk/dbms_sdk.h"
#include "sdk/tablet_sdk.h"
%}

%include "sdk/base.h"
%include "sdk/result_set.h"
%include "sdk/dbms_sdk.h"
%include "sdk/tablet_sdk.h"
