//
// file_util.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-11-28
//


#ifndef RTIDB_SET_H
#define RTIDB_SET_H

#include <set>
#include <mutex>
namespace rtidb {
namespace base {
// thread_safe set
template<class T>
class set {
public:
    set() : set_(), mu_() {}
    set(const set&) = delete;
    set& operator=(const set&) = delete;

    void insert(const T& value) {
        std::lock_guard<std::mutex> lock(mu_);
        set_.insert(value);
    }

    void erase(const T& value) {
        std::lock_guard<std::mutex> lock(mu_);
        set_.erase(value);
    }

    bool contain(const T& value) {
        std::lock_guard<std::mutex> lock(mu_);
        return set_.find(value) != set_.end();
    }

private:
    std::set<T> set_;
    std::mutex mu_;
};

}
}
#endif
