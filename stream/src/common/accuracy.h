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

#ifndef STREAM_SRC_COMMON_ACCURACY_H_
#define STREAM_SRC_COMMON_ACCURACY_H_

#include <memory>
#include <vector>

#include "common/element.h"

namespace streaming {
namespace interval_join {

double CalculateAccuracy(const std::vector<std::shared_ptr<Element>>& correct_res,
                         const std::vector<std::shared_ptr<Element>>& res);

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_COMMON_ACCURACY_H_
