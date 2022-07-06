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

#ifndef SRC_BASE_TASKPOOL_HPP_
#define SRC_BASE_TASKPOOL_HPP_

#include <vector>
#include <boost/function.hpp>
#include "base/ringqueue.h"

namespace openmldb {
namespace base {
class TaskPool {
 public:
    TaskPool(uint32_t thread_num, uint32_t qsize)
        : stop_(false), threads_num_(thread_num), queue_(qsize) {
        Start();
    }

    ~TaskPool() { Stop(); }
    typedef boost::function<void()> Task;

    bool Start() {
        for (uint32_t i = 0; i < threads_num_; i++) {
            pthread_t tid;
            int ret = pthread_create(&tid, NULL, ThreadWrapper, this);
            if (ret) {
                abort();
            }
            tids_.push_back(tid);
        }
        return true;
    }

    bool Stop() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop_ = true;
            work_cv_.notify_all();
            queue_cv_.notify_all();
        }
        for (uint32_t i = 0; i < tids_.size(); i++) {
            pthread_join(tids_[i], NULL);
        }
        tids_.clear();
        return true;
    }

    void AddTask(const Task& task) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (queue_.full() && !stop_) {
            queue_cv_.wait(lock);
        }
        if (stop_) return;
        queue_.put(task);
        work_cv_.notify_one();
    }

 private:
    static void* ThreadWrapper(void* arg) {
        reinterpret_cast<TaskPool*>(arg)->ThreadProc();
        return NULL;
    }
    void ThreadProc() {
        while (true) {
            Task task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                while (queue_.empty() && !stop_) {
                    work_cv_.wait(lock);
                }
                if (stop_ && queue_.empty()) {
                    break;
                }
                if (!queue_.empty()) {
                    task = queue_.pop();
                }
                queue_cv_.notify_one();
            }
            task();
        }
    }
    bool stop_;
    uint32_t threads_num_;
    ::openmldb::base::RingQueue<Task> queue_;
    std::vector<pthread_t> tids_;
    std::condition_variable work_cv_, queue_cv_;
    std::mutex mutex_;
};
}  // namespace base
}  // namespace openmldb
#endif  // SRC_BASE_TASKPOOL_HPP_
