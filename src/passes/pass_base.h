/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * pass_base.h
 *
 * Author: chenjing
 * Date: 2020/3/13
 *--------------------------------------------------------------------------
 **/
#include "base/fe_object.h"
#include "base/fe_status.h"

#ifndef SRC_PASSES_PASS_BASE_H_
#define SRC_PASSES_PASS_BASE_H_

namespace fesql {
namespace passes {

template <typename T, typename CTX>
class PassBase : public base::FeBaseObject {
 public:
    PassBase() = default;
    virtual ~PassBase() {}

    virtual base::Status Apply(CTX* ctx, T* input, T** out) = 0;
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_PASS_BASE_H_
