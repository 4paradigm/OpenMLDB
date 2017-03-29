/*
 * port.h
 * Copyright 2017 elasticlog <elasticlog01@gmail.com> 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef RTIDB_PORT_H
#define RTIDB_PORT_H

#include <cstdlib>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

namespace rtidb {
namespace port {

typedef pthread_once_t OnceType;
#define RTIDB_ONCE_INIT PTHREAD_ONCE_INIT
static const bool kLittleEndian = true;

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

static void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("once", pthread_once(once, initializer));
}

}
}

#endif /* !PORT_H */

/* vim: set expandtab ts=2 sw=2 sts=2 tw=100: */
