//
//  Created by wangbao on 22/10/19
//

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//  
//  Copyright (c) 2011 The LevelDB Authors. All rights reserved.
//  Use of this source code is governed by a BSD-style license that can be
//  found in the LICENSE file. See the AUTHORS file for names of contributors.

//
//  SpinMutex has very low overhead for low-contention cases.  Method names
//  are chosen so you can use std::unique_lock or std::lock_guard with it.
//

#pragma once

namespace rtidb {
namespace base {

inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
    asm volatile("pause");
#elif defined(__aarch64__)
    asm volatile("wfe");
#elif defined(__powerpc64__)
    asm volatile("or 27,27,27");
#endif
    // it's okay for other platforms to be no-ops
}

class SpinMutex {
public:
    SpinMutex() : locked_(false) {}

    bool try_lock() {
        auto currently_locked = locked_.load(std::memory_order_relaxed);
        return !currently_locked &&
            locked_.compare_exchange_weak(currently_locked, true,
                    std::memory_order_acquire,
                    std::memory_order_relaxed);
    }

    void lock() {
        for (size_t tries = 0;; ++tries) {
            if (try_lock()) {
                // success
                break;
            }
            AsmVolatilePause();
            if (tries > 100) {
                std::this_thread::yield();
            }
        }
    }

    void unlock() { locked_.store(false, std::memory_order_release); }

private:
    std::atomic<bool> locked_;
};

}
}
