/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window.h
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_WINDOW_H
#define SRC_CODEGEN_WINDOW_H
namespace fesql {
namespace codegen {
template <class V>
class Iterator{
 public:
    Iterator() {}
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
    virtual ~Iterator() {}
    virtual bool Valid() const = 0;
    virtual V Next() = 0;
    virtual void reset() = 0;
};
}  // namespace codegen
}  // namespace fesql

#endif  // SRC_CODEGEN_WINDOW_H
