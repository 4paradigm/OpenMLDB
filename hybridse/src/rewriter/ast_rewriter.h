/**
 * Copyright (c) 2024 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_REWRITER_AST_REWRITER_H_
#define HYBRIDSE_SRC_REWRITER_AST_REWRITER_H_

#include <string>

#include "absl/status/statusor.h"

namespace hybridse {
namespace rewriter {

absl::StatusOr<std::string> Rewrite(absl::string_view query);

}  // namespace rewriter
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_REWRITER_AST_REWRITER_H_
