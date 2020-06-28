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
#include "proto/fe_type.pb.h"
#include "vm/jit.h"

namespace fesql {
namespace udf {
namespace v1 {

template <class V>
int64_t count_list(int8_t *input);

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
V minimum(V l, V r);
template <class V>
V maximum(V l, V r);

template <class V>
bool has_next(int8_t *input);

template <class V>
V next_iterator(int8_t *input);

template <class V>
void delete_iterator(int8_t *input);

template <class V>
bool at_struct_list(int8_t *input, int32_t pos, V *v);
template <class V>
bool next_struct_iterator(int8_t *input, V *v);
template <class V>
bool sum_struct_list(int8_t *input, V *v);
template <class V>
bool avg_struct_list(int8_t *input, V *v);
template <class V>
bool max_strcut_list(int8_t *input, V *v);
template <class V>
bool min_struct_list(int8_t *input, V *v);
template <class V>
inline V inc(V i);
int32_t dayofmonth(int64_t ts);
int32_t month(int64_t ts);
int32_t year(int64_t ts);
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
bool RegisterUDFToModule(::llvm::Module *m);
void RegisterNativeUDFToModule(::llvm::Module *m);
}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_H_
