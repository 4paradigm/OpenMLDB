/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window.h
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BASE_WINDOW_H
#define SRC_BASE_WINDOW_H
#include "stdint.h"
namespace fesql {
namespace base {
template <class V>
class Iterator {
 public:
    Iterator() {}
    virtual ~Iterator() {}
    virtual bool Valid() const = 0;
    virtual V Next() = 0;
    virtual void reset() = 0;
};

struct Row {
    int8_t *buf;
};
}  // namespace base
}  // namespace fesql

#endif  // SRC_BASE_WINDOW_H
