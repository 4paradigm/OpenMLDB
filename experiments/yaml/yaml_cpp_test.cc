/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * yaml_cpp_test.cc
 *
 * Author: chenjing
 * Date: 2020/4/24
 *--------------------------------------------------------------------------
 **/
#include <string>
#include "boost/filesystem/operations.hpp"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "yaml-cpp/yaml.h"
namespace fesql {
namespace base {

class YamlTest : public ::testing::Test {
 public:
    YamlTest() {}
    ~YamlTest() {}
};

TEST_F(YamlTest, yaml_parse_test) {
    boost::filesystem::path current_path(boost::filesystem::current_path());
    boost::filesystem::path case_dir = current_path.parent_path().parent_path();
    std::cout << "Current path is : " << current_path << std::endl;
    std::cout << "Fesql dir path is : " << case_dir << std::endl;
    YAML::Node config =
        YAML::LoadFile(case_dir.string() + "/case/query/0/case1.yaml");
    if (config["SQLCase"]) {
        auto sql_case = config["SQLCase"];
        ASSERT_EQ(1, sql_case["id"].as<int32_t>());
        ASSERT_EQ("select col0, col1, col2, col3, col4, col5, col6 from t1;",
                  sql_case["sql"].as<std::string>());
        ASSERT_EQ(
            "col0:string, col1:int32, col2:int16, col3:float, col4:double, "
            "col5:int64, col6:string",
            sql_case["input"]["schema"].as<std::string>());
        ASSERT_EQ(
            "col0, col1, col2, col3, col4, col5, col6\n0, 1, 5, 1.1f, 11.1, 1, "
            "1\n0, 2, 5, 2.2f, 22.2, 2, 22\n0, 3, 55, 3.3f, 33.3, 1, 333\n0, "
            "4, 55, 4.4f, 44.4, 2, 4444\n0, 5, 55, 5.5f, 55.5, 3, "
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a\n",
            sql_case["input"]["data"].as<std::string>());
    } else {
        FAIL();
    }
}

}  // namespace base
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}