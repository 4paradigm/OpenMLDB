/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <common/timer.h>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/column_element.h"
#include "storage/table.h"

namespace streaming {
namespace interval_join {

class TableTest : public ::testing::Test {};

TEST_F(TableTest, TableSmokeTest) {
    Element ele1("key", "value", 1);
    Element ele2("key", "value", 2);
    SkipListTable table;
    table.Put("key", ele1.ts(), &ele1);
    table.Put("key", ele2.ts(), &ele2);

    Element* out = nullptr;
    EXPECT_TRUE(table.Get("key", 1, &out));
    EXPECT_EQ(&ele1, out);
    EXPECT_TRUE(table.Get("key", 2, &out));
    EXPECT_EQ(&ele2, out);

    EXPECT_FALSE(table.Get("key", 3, &out));
    EXPECT_EQ(nullptr, out);

    EXPECT_FALSE(table.Get("key", 0, &out));
    EXPECT_EQ(nullptr, out);

    EXPECT_FALSE(table.Get("key2", 0, &out));
    EXPECT_EQ(nullptr, out);
}

TEST_F(TableTest, TableIteratorTest) {
    SkipListTable table;
    int key_num = 10;
    int ts_num = 100;
    std::vector<std::unique_ptr<Element>> eles;
    // ts will be from 2 to 200
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            uint64_t ts = 2 + j * 2;
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(ts);
            eles.emplace_back(std::make_unique<Element>(key, value, ts));
            table.Put(key, ts, eles.back().get());
        }
    }

    Element* out = nullptr;
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            EXPECT_TRUE(table.Get(eles[i]->key(), eles[i]->ts(), &out));
            EXPECT_EQ(eles[i].get(), out);
        }
    }

    Ticket ticket;
    std::string pk = "key_2";
    auto it = table.NewIterator(pk, ticket);
    uint64_t ts = 50;
    it.Seek(ts);
    int count = 0;
    int expect_count = ts / 2;
    while (it.Valid()) {
        EXPECT_EQ(ts, it.GetKey());
        EXPECT_EQ("value_" + std::to_string(ts), it.GetValue());
        ts -= 2;
        it.Next();
        count++;
    }
    EXPECT_EQ(expect_count, count);

    ts = 49;
    it.Seek(ts);
    count = 0;
    expect_count = ts / 2;
    ts--;  // seek will seek to 48 which is just less than 49
    while (it.Valid()) {
        EXPECT_EQ(ts, it.GetKey());
        EXPECT_EQ("value_" + std::to_string(ts), it.GetValue());
        ts -= 2;
        it.Next();
        count++;
    }
    EXPECT_EQ(expect_count, count);

    ts = 200;
    it.Seek(ts);
    count = 0;
    expect_count = ts / 2;
    while (it.Valid()) {
        EXPECT_EQ(ts, it.GetKey());
        EXPECT_EQ("value_" + std::to_string(ts), it.GetValue());
        ts -= 2;
        it.Next();
        count++;
    }
    EXPECT_EQ(expect_count, count);

    ts = 300;
    it.Seek(ts);
    ts = 200;  // seek will seek to 200 which is the max ts
    count = 0;
    expect_count = ts / 2;
    while (it.Valid()) {
        EXPECT_EQ(ts, it.GetKey());
        EXPECT_EQ("value_" + std::to_string(ts), it.GetValue());
        ts -= 2;
        it.Next();
        count++;
    }
    EXPECT_EQ(expect_count, count);

    ts = 1;
    // min ts is 2, so seek(1) will get invalid iterator
    it.Seek(ts);
    EXPECT_FALSE(it.Valid());

    it.SeekToFirst();
    EXPECT_EQ(200, it.GetKey());
    EXPECT_EQ("value_200", it.GetValue());

    // pop if the iterator ends
    ticket.Pop();

    auto it2 = table.NewIterator("non_exists", ticket);
    EXPECT_TRUE(it2.Null());
}

TEST_F(TableTest, TableExpiryKeyTest) {
    SkipListTable table;
    int key_num = 10;
    int ts_num = 100;
    std::vector<std::unique_ptr<Element>> eles;
    // ts will be from 2 to 200
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            uint64_t ts = 2 + j * 2;
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(ts);
            eles.emplace_back(std::make_unique<Element>(key, value, ts));
            table.Put(key, ts, eles.back().get());
        }
    }

    Ticket ticket;
    std::string pk = "key_2";

    // expire elements <= 150
    {
        auto to_del = table.Expire(pk, 150);
        auto it = table.NewIterator(pk, ticket);
        it.SeekToFirst();
        int count = 0;
        int expect_count = 25;
        uint64_t ts = 200;
        while (it.Valid()) {
            EXPECT_EQ(ts, it.GetKey());
            EXPECT_EQ(eles[2 * ts_num + (ts / 2 - 1)].get(), it.GetRawValue());
            ts -= 2;
            it.Next();
            count++;
        }
        EXPECT_EQ(expect_count, count);

        count = 0;
        expect_count = 75;
        ts = 150;
        while (to_del) {
            EXPECT_EQ(ts, to_del->GetKey());
            EXPECT_EQ(eles[2 * ts_num + (ts / 2 - 1)].get(), to_del->GetValue());
            to_del = to_del->GetNext(0);
            count++;
            ts -= 2;
        }
        EXPECT_EQ(expect_count, count);

        ticket.Pop();
    }

    // expire < 181
    {
        auto to_del = table.Expire(pk, 181);
        auto it = table.NewIterator(pk, ticket);
        it.SeekToFirst();
        int count = 0;
        int expect_count = 10;
        uint64_t ts = 200;
        while (it.Valid()) {
            EXPECT_EQ(ts, it.GetKey());
            EXPECT_EQ(eles[2 * ts_num + (ts / 2 - 1)].get(), it.GetRawValue());
            ts -= 2;
            it.Next();
            count++;
        }
        EXPECT_EQ(expect_count, count);

        count = 0;
        expect_count = 15;
        ts = 180;
        while (to_del) {
            EXPECT_EQ(ts, to_del->GetKey());
            EXPECT_EQ(eles[2 * ts_num + (ts / 2 - 1)].get(), to_del->GetValue());
            to_del = to_del->GetNext(0);
            count++;
            ts -= 2;
        }
        EXPECT_EQ(expect_count, count);

        ticket.Pop();
    }

    // expire all elements
    {
        auto to_del = table.Expire(pk, 200);
        auto it = table.NewIterator(pk, ticket);
        EXPECT_FALSE(it.Valid());
        it.SeekToFirst();
        EXPECT_FALSE(it.Valid());

        int count = 0;
        int expect_count = 10;
        uint64_t ts = 200;
        while (to_del) {
            EXPECT_EQ(ts, to_del->GetKey());
            EXPECT_EQ(eles[2 * ts_num + (ts / 2 - 1)].get(), to_del->GetValue());
            to_del = to_del->GetNext(0);
            count++;
            ts -= 2;
        }
        EXPECT_EQ(expect_count, count);

        ticket.Pop();
    }

    // expire empty list
    {
        auto to_del = table.Expire(pk, 300);
        auto it = table.NewIterator(pk, ticket);
        EXPECT_FALSE(it.Valid());
        it.SeekToFirst();
        EXPECT_FALSE(it.Valid());

        int count = 0;
        int expect_count = 0;
        uint64_t ts = 200;
        while (to_del) {
            EXPECT_EQ(ts, to_del->GetKey());
            EXPECT_EQ(eles[2 * ts_num + (ts / 2 - 1)].get(), to_del->GetValue());
            to_del = to_del->GetNext(0);
            count++;
            ts -= 2;
        }
        EXPECT_EQ(expect_count, count);

        ticket.Pop();
    }
}

TEST_F(TableTest, TableExpiryTest) {
    SkipListTable table;
    int key_num = 10;
    int ts_num = 100;
    std::vector<std::unique_ptr<Element>> eles;
    // ts will be from 2 to 200
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            uint64_t ts = 2 + j * 2;
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(ts);
            eles.emplace_back(std::make_unique<Element>(key, value, ts));
            table.Put(key, ts, eles.back().get());
        }
    }

    Ticket ticket;
    // expire elements <= 150
    {
        auto to_del_list = table.Expire(150);
        for (int i = 0; i < key_num; i++) {
            auto to_del = to_del_list[i];
            std::string pk = "key_" + std::to_string(i);
            auto it = table.NewIterator(pk, ticket);
            it.SeekToFirst();
            int count = 0;
            int expect_count = 25;
            uint64_t ts = 200;
            while (it.Valid()) {
                EXPECT_EQ(ts, it.GetKey());
                EXPECT_EQ(eles[i * ts_num + (ts / 2 - 1)].get(), it.GetRawValue());
                ts -= 2;
                it.Next();
                count++;
            }
            EXPECT_EQ(expect_count, count);

            count = 0;
            expect_count = 75;
            ts = 150;
            while (to_del) {
                EXPECT_EQ(ts, to_del->GetKey());
                EXPECT_EQ(eles[i * ts_num + (ts / 2 - 1)].get(), to_del->GetValue());
                to_del = to_del->GetNext(0);
                count++;
                ts -= 2;
            }
            EXPECT_EQ(expect_count, count);
        }

        ticket.Pop();
    }
}

TEST_F(TableTest, TableDeleteTest) {
    SkipListTable table;
    int key_num = 10;
    int ts_num = 100;
    std::vector<std::unique_ptr<Element>> eles;
    // ts will be from 2 to 200
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            uint64_t ts = 2 + j * 2;
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(ts);
            eles.emplace_back(std::make_unique<Element>(key, value, ts));
            table.Put(key, ts, eles.back().get());
        }
    }

    Ticket ticket;
    std::string pk = "key_2";
    table.Delete(pk);
    Element* out;
    EXPECT_FALSE(table.Get(pk, 2, &out));
    auto it = table.NewIterator(pk, ticket);
    EXPECT_TRUE(it.Null());
}

TEST_F(TableTest, ReadWriteThreadTest) {
    int key_num = 1000;
    int ts_num = 10000;
    std::vector<std::unique_ptr<Element>> eles;
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            uint64_t ts = j;
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(ts);
            eles.emplace_back(std::make_unique<Element>(key, value, ts));
        }
    }

    SkipListTable table;
    auto put = [&]() {
        for (int i = 0; i < key_num; i++) {
            for (int j = 0; j < ts_num; j++) {
                auto ele = eles.at(i * ts_num + j).get();
                table.Put(ele->key(), ele->ts(), ele);
            }
        }
    };

    auto get = [&]() {
        for (int i = 0; i < key_num; i++) {
            for (int j = 0; j < ts_num; j++) {
                Element* out;
                auto ele = eles.at(i * ts_num + j).get();
                while (!table.Get(ele->key(), ele->ts(), &out)) {}
                EXPECT_EQ(ele, out);
            }
        }
    };

    auto iterate = [&]() {
        unsigned seed = ::baidu::common::timer::get_micros();
        for (int i = 0; i < key_num; i++) {
            auto ele = eles.at(i * ts_num).get();
            Ticket ticket;
            auto it = table.NewIterator(ele->key(), ticket);
            if (it.Null()) continue;

            if (i % 2 == 0) {
                it.SeekToFirst();
            } else {
                it.Seek(rand_r(&seed) % ts_num);
            }

            int count = 0;
            while (it.Valid()) {
                auto ts = it.GetKey();
                EXPECT_TRUE(ts >= 0 && ts < ts_num);
                count++;
                it.Next();
            }
            EXPECT_LE(count, ts_num);

            ticket.Pop();
        }
    };

    int get_thread_num = 8;
    int it_thread_num = 8;
    std::thread put_thread(put);
    std::vector<std::thread> get_threads;
    std::vector<std::thread> it_threads;
    for (int i = 0; i < get_thread_num; i++) {
        get_threads.emplace_back(get);
    }
    for (int i = 0; i < it_thread_num; i++) {
        it_threads.emplace_back(iterate);
    }

    ::baidu::common::timer::AutoTimer timer(1, "multi-thread");
    put_thread.join();
    for (int i = 0; i < get_thread_num; i++) {
        get_threads.at(i).join();
    }
    for (int i = 0; i < it_thread_num; i++) {
        it_threads.at(i).join();
    }
}

TEST_F(TableTest, ReadDeleteThreadTest) {
    int key_num = 1000;
    int ts_num = 10000;
    std::vector<std::unique_ptr<Element>> eles;
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            uint64_t ts = j;
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(ts);
            eles.emplace_back(std::make_unique<Element>(key, value, ts));
        }
    }

    SkipListTable table;
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            auto ele = eles.at(i * ts_num + j).get();
            table.Put(ele->key(), ele->ts(), ele);
        }
    }

    auto expire = [&]() {
        unsigned seed = ::baidu::common::timer::get_micros();
        for (int i = 0; i < key_num; i++) {
            for (int j = 0; j < ts_num; j++) {
                auto ele = eles.at(i * ts_num + j).get();
                table.Expire(ele->key(), rand_r(&seed) % (ts_num * 2));
            }
        }
    };

    auto get = [&]() {
        for (int i = 0; i < key_num; i++) {
            for (int j = 0; j < ts_num; j++) {
                Element* out;
                auto ele = eles.at(i * ts_num + j).get();
                table.Get(ele->key(), ele->ts(), &out);
                if (out != nullptr) {
                    EXPECT_EQ(ele, out);
                }
            }
        }
    };

    auto iterate = [&]() {
        unsigned seed = ::baidu::common::timer::get_micros();
        for (int i = 0; i < key_num; i++) {
            auto ele = eles.at(i * ts_num).get();
            Ticket ticket;
            auto it = table.NewIterator(ele->key(), ticket);
            if (it.Null()) continue;

            if (i % 2 == 0) {
                it.SeekToFirst();
            } else {
                it.Seek(rand_r(&seed) % ts_num);
            }

            int count = 0;
            while (it.Valid()) {
                auto ts = it.GetKey();
                EXPECT_TRUE(ts >= 0 && ts < ts_num);
                count++;
                it.Next();
            }
            EXPECT_LE(count, ts_num);

            ticket.Pop();
        }
    };

    int get_thread_num = 8;
    int it_thread_num = 8;
    std::thread put_thread(expire);
    std::vector<std::thread> get_threads;
    std::vector<std::thread> it_threads;
    for (int i = 0; i < get_thread_num; i++) {
        get_threads.emplace_back(get);
    }
    for (int i = 0; i < it_thread_num; i++) {
        it_threads.emplace_back(iterate);
    }

    ::baidu::common::timer::AutoTimer timer(1, "multi-thread");
    put_thread.join();
    for (int i = 0; i < get_thread_num; i++) {
        get_threads.at(i).join();
    }
    for (int i = 0; i < it_thread_num; i++) {
        it_threads.at(i).join();
    }
}

TEST_F(TableTest, PerfTest) {
    int key_num = 1000;
    int ts_num = 10000;
    std::vector<std::unique_ptr<Element>> eles;
    for (int i = 0; i < key_num; i++) {
        for (int j = 0; j < ts_num; j++) {
            uint64_t ts = j;
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(ts);
            eles.emplace_back(std::make_unique<Element>(key, value, ts));
        }
    }

    // hash-map based impl
    {
        std::unordered_map<std::string, std::unordered_map<uint64_t, Element*>> table;
        auto put = [&]() {
            for (int i = 0; i < key_num; i++) {
                for (int j = 0; j < ts_num; j++) {
                    auto ele = eles.at(i * ts_num + j).get();
                    auto it = table.find(ele->key());
                    if (it != table.end()) {
                        it->second[ele->ts()] = ele;
                    } else {
                        table.insert(
                            std::make_pair(ele->key(), std::unordered_map<uint64_t, Element*>({{ele->ts(), ele}})));
                    }
                }
            }
        };

        auto get = [&]() {
            for (int i = 0; i < key_num; i++) {
                for (int j = 0; j < ts_num; j++) {
                    Element* out;
                    auto ele = eles.at(i * ts_num + j).get();
                    auto it = table.find(ele->key());
                    while (it == table.end()) {
                        it = table.find(ele->key());
                    }

                    auto iit = it->second.find(ele->ts());
                    while (iit == it->second.end()) {
                        auto iit = it->second.find(ele->ts());
                    }
                    EXPECT_EQ(ele, iit->second);
                }
            }
        };
        ::baidu::common::timer::AutoTimer timer(1, "hash-table");
        auto start = ::baidu::common::timer::get_micros();
        put();
        auto end = ::baidu::common::timer::get_micros();
        LOG(WARNING) << "hash-table put takes " << (end - start) / 1000 << " ms";
        get();
        start = ::baidu::common::timer::get_micros();
        LOG(WARNING) << "hash-table get takes " << (start - end) / 1000 << " ms";
    }

    // skip-list based implementation
    {
        SkipListTable table;
        auto put = [&]() {
            for (int i = 0; i < key_num; i++) {
                for (int j = 0; j < ts_num; j++) {
                    auto ele = eles.at(i * ts_num + j).get();
                    table.Put(ele->key(), ele->ts(), ele);
                }
            }
        };

        auto get = [&]() {
            for (int i = 0; i < key_num; i++) {
                for (int j = 0; j < ts_num; j++) {
                    Element* out;
                    auto ele = eles.at(i * ts_num + j).get();
                    while (!table.Get(ele->key(), ele->ts(), &out)) {
                    }
                    EXPECT_EQ(ele, out);
                }
            }
        };
        ::baidu::common::timer::AutoTimer timer(1, "skip-list");
        auto start = ::baidu::common::timer::get_micros();
        put();
        auto end = ::baidu::common::timer::get_micros();
        LOG(WARNING) << "skip-list put takes " << (end - start) / 1000 << " ms";
        get();
        start = ::baidu::common::timer::get_micros();
        LOG(WARNING) << "skip-list get takes " << (start - end) / 1000 << " ms";
    }
}

TEST_F(TableTest, CombinedIteratorTest) {
    SkipListTable table1;
    int key_num = 1;
    int ts_num = 100;
    std::string key = "key";
    std::vector<std::unique_ptr<Element>> eles;
    // ts will be from 2, 4, 200
    for (int j = 0; j < ts_num; j++) {
        uint64_t ts = 2 + j * 2;
        std::string value = "value_" + std::to_string(ts);
        eles.emplace_back(std::make_unique<Element>(key, value, ts));
        table1.Put(key, ts, eles.back().get());
    }

    SkipListTable table2;
    // ts will be from 1, 3 to 199
    for (int j = 0; j < ts_num; j++) {
        uint64_t ts = 1 + j * 2;
        std::string value = "value_" + std::to_string(ts);
        eles.emplace_back(std::make_unique<Element>(key, value, ts));
        table2.Put(key, ts, eles.back().get());
    }

    Ticket ticket;
    auto it1 = table1.NewIterator(key, ticket);
    auto it2 = table2.NewIterator(key, ticket);
    CombinedIterator it({it1, it2});

    uint64_t ts = 50;
    it.Seek(ts);
    int count = 0;
    int expect_count = ts;
    while (it.Valid()) {
        EXPECT_EQ(ts, it.GetKey());
        EXPECT_EQ("value_" + std::to_string(ts), it.GetValue());
        ts--;
        it.Next();
        count++;
    }
    EXPECT_EQ(expect_count, count);

    ts = 49;
    it.Seek(ts);
    count = 0;
    expect_count = ts;
    while (it.Valid()) {
        EXPECT_EQ(ts, it.GetKey());
        EXPECT_EQ("value_" + std::to_string(ts), it.GetValue());
        ts--;
        it.Next();
        count++;
    }
    EXPECT_EQ(expect_count, count);

    ts = 200;
    it.Seek(ts);
    count = 0;
    expect_count = ts;
    while (it.Valid()) {
        EXPECT_EQ(ts, it.GetKey());
        EXPECT_EQ("value_" + std::to_string(ts), it.GetValue());
        ts--;
        it.Next();
        count++;
    }
    EXPECT_EQ(expect_count, count);

    ts = 300;
    it.Seek(ts);
    ts = 200;  // seek will seek to 200 which is the max ts
    count = 0;
    expect_count = ts;
    while (it.Valid()) {
        EXPECT_EQ(ts, it.GetKey());
        EXPECT_EQ("value_" + std::to_string(ts), it.GetValue());
        ts--;
        it.Next();
        count++;
    }
    EXPECT_EQ(expect_count, count);

    ts = 0;
    // min ts is 1, so seek(1) will get invalid iterator
    it.Seek(ts);
    EXPECT_FALSE(it.Valid());

    it.SeekToFirst();
    EXPECT_EQ(200, it.GetKey());
    EXPECT_EQ("value_200", it.GetValue());

    // pop if the iterator ends
    ticket.Pop();
    ticket.Pop();
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
