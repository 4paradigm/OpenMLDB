//
// rtidb_wrapper.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-25
//

#include "rtidb_wrapper.h"

#include <stdio.h>


using namespace rtidb::sdk;

extern "C" {

bool Put() {
    printf("call put\n");
    return false;
}

}

