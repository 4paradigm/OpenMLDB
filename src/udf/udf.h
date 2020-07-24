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
#include "codec/list_iterator_codec.h"
#include "codec/type_codec.h"
#include "proto/fe_type.pb.h"
#include "vm/jit.h"

namespace fesql {
namespace udf {
namespace v1 {

template <class V>
double avg_list(int8_t *input);

template <class V>
struct AtList {
    V operator()(::fesql::codec::ListRef<V> *list_ref, int32_t pos) {
        auto list = (codec::ListV<V> *)(list_ref->list);
        return list->At(pos);
    }
};

template <class V>
struct AtStructList {
    void operator()(::fesql::codec::ListRef<V> *list_ref, int32_t pos, V *v) {
        *v = AtList<V>()(list_ref, pos);
    }
};

template <class V>
struct Minimum {
    V operator()(V l, V r) { return l < r ? l : r; }
};

template <class V>
struct Maximum {
    V operator()(V l, V r) { return l > r ? l : r; }
};

template <class V>
struct StructMinimum {
    V *operator()(V *l, V *r) { return *l < *r ? l : r; }
};

template <class V>
struct StructMaximum {
    V *operator()(V *l, V *r) { return *l > *r ? l : r; }
};

template <class V>
bool iterator_list(int8_t *input, int8_t *output);

template <class V>
bool has_next(int8_t *input);

template <class V>
V next_iterator(int8_t *input);

template <class V>
void delete_iterator(int8_t *input);

template <class V>
bool next_struct_iterator(int8_t *input, V *v);

template <class V>
struct IncOne {
    V operator()(V i) { return i + 1; }
};

int32_t month(int64_t ts);
int32_t month(fesql::codec::Timestamp *ts);

int32_t year(int64_t ts);
int32_t year(fesql::codec::Timestamp *ts);

int32_t dayofmonth(int64_t ts);
int32_t dayofmonth(fesql::codec::Timestamp *ts);

int32_t dayofweek(int64_t ts);
int32_t dayofweek(fesql::codec::Timestamp *ts);
int32_t dayofweek(fesql::codec::Date *ts);

int32_t weekofyear(int64_t ts);
int32_t weekofyear(fesql::codec::Timestamp *ts);
int32_t weekofyear(fesql::codec::Date *ts);

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
void ClearNativeUDFDict();
void RegisterNativeUDFToModule(::llvm::Module *m);
}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_H_
