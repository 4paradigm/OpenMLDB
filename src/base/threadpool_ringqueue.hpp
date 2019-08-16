//
// Created by kongsys on 8/16/19.
//

#ifndef RTIDB_THREADPOOL_RINGQUEUE_HPP
#define RTIDB_THREADPOOL_RINGQUEUE_HPP

#include <boost/function.hpp>
#include "base/ringqueue.h"

namespace rtidb {
namespace base {
class ThreadPool_RingQueue {
    public:
        ThreadPool_RingQueue(uint32_t thread_num, uint32_t qsize):
        stop_(false),
        threads_num_(thread_num),
        queue_(qsize) {
            Start();
        };

        ~ThreadPool_RingQueue() {
            Stop();
        };
    typedef boost::function<void ()> Task;

    bool Start() {
        for(uint32_t i = 0; i < threads_num_; i++) {
            pthread_t tid;
            int ret = pthread_create(&tid, NULL, ThreadWrapper, this);
            if (ret) {
                abort();
            }
            tids_.push_back(tid);
        }
        return true;
    };

    bool Stop() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop_ = true;
        }
        for (uint32_t i = 0; i < tids_.size(); i++) {
            pthread_join(tids_[i], NULL);
        }
        tids_.clear();
        return true;
    };

    void AddTask(const Task& task) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (stop_) return;
        queue_.put(task);
        work_cv_.notify_one();
    };
    private:
        static void* ThreadWrapper(void* arg) {
            reinterpret_cast<ThreadPool_RingQueue*>(arg)->ThreadProc();
            return NULL;
        }
        void ThreadProc() {
            while (true) {
                Task task;
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
                lock.unlock();
                task();
                lock.lock();
            }
        }
    bool stop_;
    uint32_t threads_num_;
    ::rtidb::base::RingQueue<Task> queue_;
    std::vector<pthread_t> tids_;
    std::condition_variable work_cv_;
    std::mutex mutex_;
};
}
}
#endif //RTIDB_THREADPOOL_RINGQUEUE_HPP
