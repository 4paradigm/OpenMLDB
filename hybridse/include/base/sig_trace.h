/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_INCLUDE_BASE_SIG_TRACE_H_
#define HYBRIDSE_INCLUDE_BASE_SIG_TRACE_H_

#include <dlfcn.h>
#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>

namespace hybridse {
namespace base {

void FeSignalBacktraceHandler(int sig) {
    fprintf(stderr, "Receive signal %d\n", sig);

    void* trace[32];
    int trace_size = backtrace(trace, 32);
    char** messages = backtrace_symbols(trace, trace_size);
    if (messages == nullptr) {
        fprintf(stderr, "Fail to backtrace symbols");
        exit(sig);
    }
    for (int i = 0; i < trace_size; ++i) {
        Dl_info dl_info;
        void* load_base = nullptr;
        if (dladdr(trace[i], &dl_info)) {
            load_base = dl_info.dli_fbase;
        }
        if (messages[i] != nullptr) {
            if (load_base != nullptr) {
                fprintf(stderr, "[%p] %s\n", load_base, messages[i]);
            } else {
                fprintf(stderr, "[???] %s\n", messages[i]);
            }
        }
    }
    free(messages);
    exit(sig);
}

}  // namespace base
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_BASE_SIG_TRACE_H_
