/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef STREAM_SRC_COMMON_UTIL_H_
#define STREAM_SRC_COMMON_UTIL_H_

#include <string>

#include "stream/data_stream.h"

namespace streaming {
namespace interval_join {

DECLARE_int64(latency_unit);

double GetLatency(DataStream* ds, const std::string& path);
double GetJoinCounts(DataStream* ds);
double GetBaseEffectiveness(DataStream* ds);
double GetProbeEffectiveness(DataStream* ds);

}  // namespace interval_join
}  // namespace streaming


#endif  // STREAM_SRC_COMMON_UTIL_H_
