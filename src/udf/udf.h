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
#ifdef __cplusplus
extern "C" {
#endif
int32_t inc_int32(int32_t i);
int16_t sum_list_int16(int8_t *input);
int32_t sum_list_int32(int8_t *input);
int64_t sum_list_int64(int8_t *input);
float sum_list_float(int8_t *input);
double sum_list_double(int8_t *input);

int16_t max_list_int16(int8_t *input);
int32_t max_list_int32(int8_t *input);
int64_t max_list_int64(int8_t *input);
float max_list_float(int8_t *input);
double max_list_double(int8_t *input);

int16_t min_list_int16(int8_t *input);
int32_t min_list_int32(int8_t *input);
int64_t min_list_int64(int8_t *input);
float min_list_float(int8_t *input);
double min_list_double(int8_t *input);

int16_t at_list_int16(int8_t *input, int32_t pos);
int32_t at_list_int32(int8_t *input, int32_t pos);
int64_t at_list_int64(int8_t *input, int32_t pos);
float at_list_float(int8_t *input, int32_t pos);
double at_list_double(int8_t *input, int32_t pos);

bool iterator_list_int16(int8_t *input, int8_t *output);
bool iterator_list_int32(int8_t *input, int8_t *output);
bool iterator_list_int64(int8_t *input, int8_t *output);
bool iterator_list_float(int8_t *input, int8_t *output);
bool iterator_list_double(int8_t *input, int8_t *output);

bool has_next_iterator_int16(int8_t *input);
bool has_next_iterator_int32(int8_t *input);
bool has_next_iterator_int64(int8_t *input);
bool has_next_iterator_float(int8_t *input);
bool has_next_iterator_double(int8_t *input);

int16_t next_iterator_int16(int8_t *input);
int32_t next_iterator_int32(int8_t *input);
int64_t next_iterator_int64(int8_t *input);
float next_iterator_float(int8_t *input);
double next_iterator_double(int8_t *input);

#ifdef __cplusplus
}
#endif
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
