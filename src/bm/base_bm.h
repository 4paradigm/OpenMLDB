/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * base_bm.h
 *
 * Author: chenjing
 * Date: 2019/12/24
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BM_BASE_BM_H_
#define SRC_BM_BASE_BM_H_

#include <random>
#include <vector>
namespace fesql {
namespace bm {
template <class T>
class Repeater {
 public:
    Repeater() : idx_(0), values_({}) {}
    explicit Repeater(T value) : idx_(0), values_({value}) {}
    explicit Repeater(const std::vector<T>& values)
        : idx_(0), values_(values) {}

    virtual T GetValue() {
        T value = values_[idx_];
        idx_ = (idx_ + 1) % values_.size();
        return value;
    }

    uint32_t idx_;
    std::vector<T> values_;
};

template <class T>
class NumberRepeater : public Repeater<T> {
 public:
    void Range(T min, T max, T step) {
        this->values_.clear();
        for (T v = min; v <= max; v += step) {
            this->values_.push_back(v);
        }
    }
};

template <class T>
class IntRepeater : public NumberRepeater<T> {
 public:
    void Random(T min, T max, int32_t random_size) {
        this->values_.clear();
        std::default_random_engine e;
        std::uniform_int_distribution<T> u(min, max);
        for (int i = 0; i < random_size; ++i) {
            this->values_.push_back(u(e));
        }
    }
};

template <class T>
class RealRepeater : public NumberRepeater<T> {
 public:
    void Random(T min, T max, int32_t random_size) {
        std::default_random_engine e;
        std::uniform_real_distribution<T> u(min, max);
        for (int i = 0; i < random_size; ++i) {
            this->values_.push_back(u(e));
        }
    }
};
}  // namespace bm
}  // namespace fesql

#endif  // SRC_BM_BASE_BM_H_
