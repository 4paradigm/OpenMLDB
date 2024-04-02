#include "refreshable_map.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>
namespace openmldb::auth {

class RefreshableMapTest : public ::testing::Test {
 protected:
    // SetUp method to initialize test cases, if needed
    virtual void SetUp() {}

    // TearDown method to clean up after tests, if needed
    virtual void TearDown() {}
};

// Test retrieving an existing key
TEST_F(RefreshableMapTest, GetExistingKey) {
    auto initialMap = std::make_unique<std::unordered_map<std::string, int>>();
    (*initialMap)["key1"] = 100;
    RefreshableMap<std::string, int> map;
    map.Refresh(std::move(initialMap));

    auto value = map.Get("key1");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value.value(), 100);
}

// Test attempting to retrieve a non-existing key
TEST_F(RefreshableMapTest, GetNonExistingKey) {
    auto initialMap = std::make_unique<std::unordered_map<std::string, int>>();
    (*initialMap)["key1"] = 100;
    RefreshableMap<std::string, int> map;
    map.Refresh(std::move(initialMap));

    auto value = map.Get("non_existing_key");
    ASSERT_FALSE(value.has_value());
}

// Test refreshing the map with new data
TEST_F(RefreshableMapTest, RefreshMap) {
    auto initialMap = std::make_unique<std::unordered_map<std::string, int>>();
    (*initialMap)["key1"] = 100;
    RefreshableMap<std::string, int> map;
    map.Refresh(std::move(initialMap));

    // Refresh with new map
    auto newMap = std::make_unique<std::unordered_map<std::string, int>>();
    (*newMap)["key2"] = 200;
    map.Refresh(std::move(newMap));

    // Old key should not exist
    auto oldKeyValue = map.Get("key1");
    ASSERT_FALSE(oldKeyValue.has_value());

    // New key should exist
    auto newKeyValue = map.Get("key2");
    ASSERT_TRUE(newKeyValue.has_value());
    EXPECT_EQ(newKeyValue.value(), 200);
}

TEST_F(RefreshableMapTest, ConcurrencySafety) {
    auto initialMap = std::make_unique<std::unordered_map<int, int>>();
    for (int i = 0; i < 100; ++i) {
        (*initialMap)[i] = i;
    }
    RefreshableMap<int, int> map;
    map.Refresh(std::move(initialMap));

    constexpr int numReaders = 10;
    constexpr int numWrites = 5;
    std::vector<std::thread> threads;

    // Launch reader threads
    threads.reserve(numReaders);
    for (int i = 0; i < numReaders; ++i) {
        threads.emplace_back([&map]() {
            for (int j = 0; j < 1000; ++j) {         // A large number to ensure overlap with writes
                auto value = map.Get(rand() % 100);  // Random key to simulate varied access
                // Optionally assert something about the read values, but here we mainly
                // care about the absence of crashes or data races.
            }
        });
    }

    // Launch writer thread
    threads.emplace_back([&map]() {
        for (int i = 0; i < numWrites; ++i) {
            auto newMap = std::make_unique<std::unordered_map<int, int>>();
            for (int j = 0; j < 100; ++j) {
                (*newMap)[j] = j + i + 1;  // Modify the values slightly on each refresh
            }
            map.Refresh(std::move(newMap));
            std::this_thread::sleep_for(std::chrono::milliseconds(10));  // Sleep to allow readers to process
        }
    });

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // If we reach this point without deadlocks, crashes, or data races,
    // the test is considered successful. However, keep in mind that
    // passing this test does not guarantee thread safety in all cases,
    // but it's a good indication that the basic mechanisms for concurrency
    // control in RefreshableMap are working as intended.
}
}  // namespace openmldb::auth
