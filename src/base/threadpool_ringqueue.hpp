//
// Created by kongsys on 8/16/19.
//

#ifndef RTIDB_THREADPOOL_RINGQUEUE_HPP
#define RTIDB_THREADPOOL_RINGQUEUE_HPP

#include <boost/function.hpp>
#include "base/ringqueue.h"

namespace rtidb {
    namespace base {
        template <class T>
        class threadpool_ringqueue {
        public:
            threadpool_ringqueue(uint32_t thread_num = 10, uint32_t qsize = 100):
            stop_(false),
            threads_num_(thread_num),
            rq(qsize) {
                Start();
            };

            ~threadpool_ringqueue() {};
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

        bool Stop(bool wait) {
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
            queue_.push_back(BGItem(0, task));
            work_cv_.notify_one();
        };
        ::rtidb::base::ringqueue<T> rq;
        private:
            static void* ThreadWrapper(void* arg) {
                reinterpret_cast<threadpool_ringqueue*>(arg)->ThreadProc();
                return NULL;
            }
            void ThreadProc() {
                Task task;
                std::unique_lock<std::mutex> lock(mutex_);
                while (queue_.empty() && !stop_) {
                    work_cv_.wait(lock);
                }
                if (!queue_.empty()) {
                    task = queue_.front().task;
                    queue_.pop_front();
                }
                lock.unlock();
                while (true) {
                    if (stop_ && rq.empty()) {
                        break;
                    }
                    task();
                }
            }
            struct BGItem {
                int64_t id;
                Task task;

                BGItem() {}
                BGItem(int64_t id_t, const Task& task_t):
                id(id_t), task(task_t) {}
            };
            std::deque<BGItem> queue_;
            bool stop_;
            uint32_t threads_num_;
            std::vector<pthread_t> tids_;
            std::condition_variable work_cv_;
            std::mutex mutex_;
        };
    }
}
#endif //RTIDB_THREADPOOL_RINGQUEUE_HPP
