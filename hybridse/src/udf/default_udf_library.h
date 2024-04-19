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

#ifndef HYBRIDSE_SRC_UDF_DEFAULT_UDF_LIBRARY_H_
#define HYBRIDSE_SRC_UDF_DEFAULT_UDF_LIBRARY_H_
#include "udf/udf_library.h"

namespace hybridse {
namespace udf {

class DefaultUdfLibrary : public UdfLibrary {
 public:
    static DefaultUdfLibrary* get();

    ~DefaultUdfLibrary() override {}

 private:
    static DefaultUdfLibrary* MakeDefaultUdf();

    DefaultUdfLibrary() { Init(); }

    void Init();
    void InitMathUdf();
    void InitStringUdf();
    void InitTrigonometricUdf();
    void InitTimeAndDateUdf();
    void InitTypeUdf();
    void InitLogicalUdf();
    void InitWindowFunctions();
    void InitUdaf();
    void InitAggByCateUdafs();
    void InitSumByCateUdafs();
    void InitCountByCateUdafs();
    void InitMinByCateUdafs();
    void initMaxByCateUdaFs();
    void InitAvgByCateUdafs();
    void InitFeatureSignature();
    void InitFeatureZero();

    // Array Udf defines, udfs either accept array as parameter or returns array
    void InitArrayUdfs();

    // Map functions
    void InitMapUdfs();

    // aggregate functions for statistic
    void InitStatisticsUdafs();

    // earth distance udfs
    void InitEarthDistanceUdf();

    void InitJsonUdfs();
};

}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_DEFAULT_UDF_LIBRARY_H_
