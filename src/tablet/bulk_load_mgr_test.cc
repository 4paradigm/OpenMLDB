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

#include "bulk_load_mgr.h"

#include "gtest/gtest.h"

namespace openmldb::tablet {
class MockBulkLoadMgr : public BulkLoadMgr {
 public:
    std::shared_ptr<DataReceiver> GetDataReceiverPub(uint32_t tid, uint32_t pid, bool create) {
        return GetDataReceiver(tid, pid, create);
    }
};

class BulkLoadMgrTest : public ::testing::Test {
 protected:
    MockBulkLoadMgr mgr;
};

TEST_F(BulkLoadMgrTest, create_data_receivers) {
    // new tid
    {
        std::vector<std::thread> workers;
        for (int i = 0; i < 5; i++) {
            workers.push_back(std::thread([this, i]() {
                ASSERT_EQ(mgr.GetDataReceiverPub(i, i, false), nullptr);
                ASSERT_NE(mgr.GetDataReceiverPub(i, i, true), nullptr);
                ASSERT_NE(mgr.GetDataReceiverPub(i, i, false), nullptr);
            }));
        }

        std::for_each(workers.begin(), workers.end(), [](std::thread& t) { t.join(); });
    }
    // new pid
    {
        std::vector<std::thread> workers;
        for (int i = 0; i < 5; i++) {
            workers.push_back(std::thread([this, i]() {
              ASSERT_EQ(mgr.GetDataReceiverPub(123, i, false), nullptr);
              ASSERT_NE(mgr.GetDataReceiverPub(123, i, true), nullptr);
              ASSERT_NE(mgr.GetDataReceiverPub(123, i, false), nullptr);
            }));
        }

        std::for_each(workers.begin(), workers.end(), [](std::thread& t) { t.join(); });
    }

}
}  // namespace openmldb::tablet

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}