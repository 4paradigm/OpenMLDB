/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * default_udf_library.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_UDF_DEFAULT_UDF_LIBRARY_H_
#define SRC_UDF_DEFAULT_UDF_LIBRARY_H_
#include "udf/udf_library.h"

namespace fesql {
namespace udf {

class DefaultUDFLibrary : public UDFLibrary {
 public:
    static DefaultUDFLibrary* get() { return &inst_; }

 private:
    void Init();
    void IniMathUDF();
    void InitStringUDF();
    void InitTrigonometricUDF();
    void InitDateUDF();
    void InitTypeUDF();
    void InitUtilityUDF();
    void InitWindowFunctions();
    void InitUDAF();
    void InitAggByCateUDAFs();
    void InitSumByCateUDAFs();
    void InitCountByCateUDAFs();
    void InitMinByCateUDAFs();
    void InitMaxByCateUDAFs();
    void InitAvgByCateUDAFs();
    void InitFeatureZero();

    static DefaultUDFLibrary inst_;

    DefaultUDFLibrary() { Init(); }
};

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_DEFAULT_UDF_LIBRARY_H_
