/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_UDF_UDF_H_
#define SRC_UDF_UDF_H_
#include <stdint.h>
#include <string>
#include "proto/type.pb.h"
#include "storage/type_native_fn.h"
namespace fesql {
namespace udf {
namespace v1 {
int32_t inc_int32(int32_t i);

template <class V>
V sum_list(int8_t *input);

template <class V>
double avg_list(int8_t *input);

template <class V>
V max_list(int8_t *input);

template <class V>
V min_list(int8_t *input);

template <class V>
V at_list(int8_t *input, int32_t pos);

template <class V>
bool iterator_list(int8_t *input, int8_t *output);

template <class V>
bool has_next_iterator(int8_t *input);

template <class V>
V next_iterator(int8_t *input);

}  // namespace v1

void InitUDFSymbol(vm::FeSQLJIT *jit_ptr);                // NOLINT
void InitUDFSymbol(::llvm::orc::JITDylib &jd,             // NOLINT
                   ::llvm::orc::MangleAndInterner &mi);   // NOLINT
void InitCLibSymbol(vm::FeSQLJIT *jit_ptr);               // NOLINT
void InitCLibSymbol(::llvm::orc::JITDylib &jd,            // NOLINT
                    ::llvm::orc::MangleAndInterner &mi);  // NOLINT
bool AddSymbol(::llvm::orc::JITDylib &jd,                 // NOLINT
               ::llvm::orc::MangleAndInterner &mi,        // NOLINT
               const std::string &fn_name, void *fn_ptr);
void RegisterUDFToModule(::llvm::Module *m);

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_H_
