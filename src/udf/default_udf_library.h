/*
 * default_udf_library.h
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
    void InitFeatureZero();

    static DefaultUDFLibrary inst_;

    DefaultUDFLibrary() { Init(); }
};

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_DEFAULT_UDF_LIBRARY_H_
