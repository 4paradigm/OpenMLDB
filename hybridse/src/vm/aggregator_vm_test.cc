/*
* Copyright 2021 4Paradigm
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include "gtest/gtest.h"
#include "proto/fe_type.pb.h"
#include "vm/aggregator.h"
#include "codec/fe_row_codec.h"

namespace hybridse {
namespace vm {

class AggregatorVMTest : public ::testing::TestWithParam<type::Type> {};
INSTANTIATE_TEST_SUITE_P(AggregatorTestTypes, AggregatorVMTest,
                         testing::Values(type::kInt16, type::kInt32, type::kInt64, type::kDate,
                                         type::kTimestamp, type::kFloat, type::kDouble, type::kVarchar));

TEST_P(AggregatorVMTest, SumTest) {
    auto agg_col_type = GetParam();
    if (agg_col_type == type::kVarchar) {
        GTEST_SKIP_("Skip kVarchar for SumAggregator: Not support");
    }

    std::unique_ptr<BaseAggregator> aggregator;
    codec::Schema schema;
    auto column = schema.Add();
    column->set_type(agg_col_type);
    column->set_name("val");

    switch (agg_col_type) {
        case type::kInt16:
        case type::kInt32:
        case type::kDate:
        case type::kTimestamp:
        case type::kInt64: {
            aggregator = std::make_unique<SumAggregator<int64_t>>(agg_col_type, schema);
            break;
        }
        case type::kFloat: {
            aggregator = std::make_unique<SumAggregator<float>>(agg_col_type, schema);
            break;
        }
        case type::kDouble: {
            aggregator = std::make_unique<SumAggregator<double>>(agg_col_type, schema);
            break;
        }
        default:
            LOG(ERROR) << "SumAggregator does not support for type " << Type_Name(agg_col_type);
            EXPECT_TRUE(false);
            break;
    }

    int pre_agg_values_size = 10;
    int expect = 0;
    for (int i = 0; i < pre_agg_values_size; i++) {
        expect += i * 2;

        std::string bval;
        switch (agg_col_type) {
            case type::kInt16:
            case type::kInt32:
            case type::kDate:
            case type::kTimestamp:
            case type::kInt64: {
                int64_t val = i;
                dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->UpdateValue(val);
                if (i % 2 == 1) {
                    val += (i - 1);
                    bval.assign(reinterpret_cast<char*>(&val), sizeof(int64_t));
                }
                break;
            }
            case type::kFloat: {
                float val = i;
                dynamic_cast<Aggregator<float>*>(aggregator.get())->UpdateValue(val);
                if (i % 2 == 1) {
                    val += (i - 1);
                    bval.assign(reinterpret_cast<char*>(&val), sizeof(float));
                }
                break;
            }
            case type::kDouble: {
                double val = i;
                dynamic_cast<Aggregator<double>*>(aggregator.get())->UpdateValue(val);
                if (i % 2 == 1) {
                    val += (i - 1);
                    bval.assign(reinterpret_cast<char*>(&val), sizeof(double));
                }
                break;
            }
            default:
                LOG(ERROR) << "SumAggregator does not support for type " << Type_Name(agg_col_type);
                EXPECT_TRUE(false);
                break;
        }

        if (i % 2 == 1) {
            aggregator->Update(bval);
        }
    }

    switch (agg_col_type) {
        case type::kInt16:
        case type::kInt32:
        case type::kDate:
        case type::kTimestamp:
        case type::kInt64: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->val());
            break;
        }
        case type::kFloat: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<float>*>(aggregator.get())->val());
            break;
        }
        case type::kDouble: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<double>*>(aggregator.get())->val());
            break;
        }
        default:
            LOG(ERROR) << "SumAggregator does not support for type " << Type_Name(agg_col_type);
            EXPECT_TRUE(false);
            break;
    }
}

TEST_P(AggregatorVMTest, CountTest) {
    auto agg_col_type = GetParam();
    std::unique_ptr<BaseAggregator> aggregator;
    codec::Schema schema;
    auto column = schema.Add();
    column->set_type(agg_col_type);
    column->set_name("val");
    aggregator = std::make_unique<CountAggregator>(agg_col_type, schema);

    int pre_agg_values_size = 10;
    int expect = 0;
    for (int i = 0; i < pre_agg_values_size; i++) {
        std::string bval;
        int64_t val = i;
        bval.assign(reinterpret_cast<char*>(&val), sizeof(int64_t));
        dynamic_cast<CountAggregator*>(aggregator.get())->UpdateValue(1);
        expect += 1;
        aggregator->Update(bval);
        expect += i;
    }
    EXPECT_EQ(expect, dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->val());
}

TEST_P(AggregatorVMTest, AvgTest) {
    auto agg_col_type = GetParam();
    if (agg_col_type == type::kVarchar) {
        GTEST_SKIP_("Skip kVarchar for AvgAggregator: Not support");
    }

    std::unique_ptr<BaseAggregator> aggregator;
    codec::Schema schema;
    auto column = schema.Add();
    column->set_type(agg_col_type);
    column->set_name("val");

    switch (agg_col_type) {
        case type::kInt16:
        case type::kInt32:
        case type::kDate:
        case type::kTimestamp:
        case type::kInt64: {
            aggregator = std::make_unique<AvgAggregator<int64_t>>(agg_col_type, schema);
            break;
        }
        case type::kFloat: {
            aggregator = std::make_unique<AvgAggregator<float>>(agg_col_type, schema);
            break;
        }
        case type::kDouble: {
            aggregator = std::make_unique<AvgAggregator<double>>(agg_col_type, schema);
            break;
        }
        default:
            LOG(ERROR) << "AvgAggregator does not support for type " << Type_Name(agg_col_type);
            EXPECT_TRUE(false);
            break;
    }

    int pre_agg_values_size = 10;
    double sum = 0;
    int64_t total_count = 0;
    for (int i = 0; i < pre_agg_values_size; i++) {
        sum += i * 2;
        total_count++;

        std::string bval;
        switch (agg_col_type) {
            case type::kInt16:
            case type::kInt32:
            case type::kDate:
            case type::kTimestamp:
            case type::kInt64: {
                int64_t val = i;
                dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->UpdateValue(val);
                if (i % 2 == 1) {
                    val += (i - 1);
                    bval.assign(reinterpret_cast<char*>(&val), sizeof(int64_t));
                }
                break;
            }
            case type::kFloat: {
                float val = i;
                dynamic_cast<Aggregator<float>*>(aggregator.get())->UpdateValue(val);
                if (i % 2 == 1) {
                    val += (i - 1);
                    bval.assign(reinterpret_cast<char*>(&val), sizeof(float));
                }
                break;
            }
            case type::kDouble: {
                double val = i;
                dynamic_cast<Aggregator<double>*>(aggregator.get())->UpdateValue(val);
                if (i % 2 == 1) {
                    val += (i - 1);
                    bval.assign(reinterpret_cast<char*>(&val), sizeof(double));
                }
                break;
            }
            default:
                LOG(ERROR) << "AvgAggregator does not support for type " << Type_Name(agg_col_type);
                EXPECT_TRUE(false);
                break;
        }
        if (i % 2 == 1) {
            int64_t count = 2;
            total_count += count;
            bval.append(reinterpret_cast<char*>(&count), sizeof(int64_t));
            aggregator->Update(bval);
        }
    }
    sum /= total_count;

    switch (agg_col_type) {
        case type::kInt16:
        case type::kInt32:
        case type::kDate:
        case type::kTimestamp:
        case type::kInt64: {
            int64_t expect = static_cast<int64_t>(sum);
            EXPECT_EQ(expect, dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->val());
            break;
        }
        case type::kFloat: {
            EXPECT_EQ(sum, dynamic_cast<Aggregator<float>*>(aggregator.get())->val());
            break;
        }
        case type::kDouble: {
            EXPECT_EQ(sum, dynamic_cast<Aggregator<double>*>(aggregator.get())->val());
            break;
        }
        default:
            LOG(ERROR) << "AvgAggregator does not support for type " << Type_Name(agg_col_type);
            EXPECT_TRUE(false);
            break;
    }
}

TEST_P(AggregatorVMTest, MinTest) {
    auto agg_col_type = GetParam();
    std::unique_ptr<BaseAggregator> aggregator;
    codec::Schema schema;
    auto column = schema.Add();
    column->set_type(agg_col_type);
    column->set_name("val");

    switch (agg_col_type) {
        case type::kInt16: {
            aggregator = std::make_unique<MinAggregator<int16_t>>(agg_col_type, schema);
            break;
        }
        case type::kInt32:
        case type::kDate: {
            aggregator = std::make_unique<MinAggregator<int32_t>>(agg_col_type, schema);
            break;
        }
        case type::kTimestamp:
        case type::kInt64: {
            aggregator = std::make_unique<MinAggregator<int64_t>>(agg_col_type, schema);
            break;
        }
        case type::kFloat: {
            aggregator = std::make_unique<MinAggregator<float>>(agg_col_type, schema);
            break;
        }
        case type::kDouble: {
            aggregator = std::make_unique<MinAggregator<double>>(agg_col_type, schema);
            break;
        }
        case type::kVarchar: {
            aggregator = std::make_unique<MinAggregator<std::string>>(agg_col_type, schema);
            break;
        }
        default:
            LOG(ERROR) << "MinAggregator does not support for type " << Type_Name(agg_col_type);
            EXPECT_TRUE(false);
            break;
    }

    int pre_agg_values_size = 10;
    int expect = 1;
    for (int i = 1; i <= pre_agg_values_size; i++) {
        std::string bval;
        switch (agg_col_type) {
            case type::kInt16: {
                int16_t val = i;
                dynamic_cast<Aggregator<int16_t>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(int16_t));
                break;
            }
            case type::kInt32:
            case type::kDate: {
                int32_t val = i;
                dynamic_cast<Aggregator<int32_t>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(int32_t));
                break;
            }
            case type::kTimestamp:
            case type::kInt64: {
                int64_t val = i;
                dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(int64_t));
                break;
            }
            case type::kFloat: {
                float val = i;
                dynamic_cast<Aggregator<float>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(float));
                break;
            }
            case type::kDouble: {
                double val = i;
                dynamic_cast<Aggregator<double>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(double));
                break;
            }
            case type::kVarchar: {
                std::string val = std::to_string(i);
                dynamic_cast<Aggregator<std::string>*>(aggregator.get())->UpdateValue(val);
                bval.swap(val);
                break;
            }
            default:
                LOG(ERROR) << "MinAggregator does not support for type " << Type_Name(agg_col_type);
                EXPECT_TRUE(false);
                break;
        }

        aggregator->Update(bval);
    }

    switch (agg_col_type) {
        case type::kInt16: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<int16_t>*>(aggregator.get())->val());
            break;
        }
        case type::kInt32:
        case type::kDate: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<int32_t>*>(aggregator.get())->val());
            break;
        }
        case type::kTimestamp:
        case type::kInt64: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->val());
            break;
        }
        case type::kFloat: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<float>*>(aggregator.get())->val());
            break;
        }
        case type::kDouble: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<double>*>(aggregator.get())->val());
            break;
        }
        case type::kVarchar: {
            EXPECT_EQ(std::to_string(expect), dynamic_cast<Aggregator<std::string>*>(aggregator.get())->val());
            break;
        }
        default:
            LOG(ERROR) << "MinAggregator does not support for type " << Type_Name(agg_col_type);
            EXPECT_TRUE(false);
            break;
    }
}

TEST_P(AggregatorVMTest, MaxTest) {
    auto agg_col_type = GetParam();
    std::unique_ptr<BaseAggregator> aggregator;
    codec::Schema schema;
    auto column = schema.Add();
    column->set_type(agg_col_type);
    column->set_name("val");

    switch (agg_col_type) {
        case type::kInt16: {
            aggregator = std::make_unique<MaxAggregator<int16_t>>(agg_col_type, schema);
            break;
        }
        case type::kInt32:
        case type::kDate: {
            aggregator = std::make_unique<MaxAggregator<int32_t>>(agg_col_type, schema);
            break;
        }
        case type::kTimestamp:
        case type::kInt64: {
            aggregator = std::make_unique<MaxAggregator<int64_t>>(agg_col_type, schema);
            break;
        }
        case type::kFloat: {
            aggregator = std::make_unique<MaxAggregator<float>>(agg_col_type, schema);
            break;
        }
        case type::kDouble: {
            aggregator = std::make_unique<MaxAggregator<double>>(agg_col_type, schema);
            break;
        }
        case type::kVarchar: {
            aggregator = std::make_unique<MaxAggregator<std::string>>(agg_col_type, schema);
            break;
        }
        default:
            LOG(ERROR) << "MaxAggregator does not support for type " << Type_Name(agg_col_type);
            EXPECT_TRUE(false);
            break;
    }

    int pre_agg_values_size = 10;
    int expect = pre_agg_values_size;
    for (int i = 1; i <= pre_agg_values_size; i++) {
        std::string bval;
        switch (agg_col_type) {
            case type::kInt16: {
                int16_t val = i;
                dynamic_cast<Aggregator<int16_t>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(int16_t));
                break;
            }
            case type::kInt32:
            case type::kDate: {
                int32_t val = i;
                dynamic_cast<Aggregator<int32_t>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(int32_t));
                break;
            }
            case type::kTimestamp:
            case type::kInt64: {
                int64_t val = i;
                dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(int64_t));
                break;
            }
            case type::kFloat: {
                float val = i;
                dynamic_cast<Aggregator<float>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(float));
                break;
            }
            case type::kDouble: {
                double val = i;
                dynamic_cast<Aggregator<double>*>(aggregator.get())->UpdateValue(val);
                bval.assign(reinterpret_cast<char*>(&val), sizeof(double));
                break;
            }
            case type::kVarchar: {
                std::string val = std::to_string(i);
                dynamic_cast<Aggregator<std::string>*>(aggregator.get())->UpdateValue(val);
                bval.swap(val);
                break;
            }
            default:
                LOG(ERROR) << "MaxAggregator does not support for type " << Type_Name(agg_col_type);
                EXPECT_TRUE(false);
                break;
        }

        aggregator->Update(bval);
    }

    switch (agg_col_type) {
        case type::kInt16: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<int16_t>*>(aggregator.get())->val());
            break;
        }
        case type::kInt32:
        case type::kDate: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<int32_t>*>(aggregator.get())->val());
            break;
        }
        case type::kTimestamp:
        case type::kInt64: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<int64_t>*>(aggregator.get())->val());
            break;
        }
        case type::kFloat: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<float>*>(aggregator.get())->val());
            break;
        }
        case type::kDouble: {
            EXPECT_EQ(expect, dynamic_cast<Aggregator<double>*>(aggregator.get())->val());
            break;
        }
        case type::kVarchar: {
            EXPECT_EQ("9", dynamic_cast<Aggregator<std::string>*>(aggregator.get())->val());
            break;
        }
        default:
            LOG(ERROR) << "MaxAggregator does not support for type " << Type_Name(agg_col_type);
            EXPECT_TRUE(false);
            break;
    }
}

TEST_F(AggregatorVMTest, NullTest) {
    auto agg_col_type = type::kInt64;
    codec::Schema schema;
    auto column = schema.Add();
    column->set_type(agg_col_type);
    column->set_name("val");
    codec::RowView row_view(schema);
    Row row;

    auto check_null = [&](BaseAggregator* aggregator) {
        EXPECT_TRUE(aggregator->IsNull());
        row = aggregator->Output();
        row_view.Reset(row.buf());
        EXPECT_TRUE(row_view.IsNULL(0));
    };

    auto check_not_null = [&](BaseAggregator* aggregator) {
        EXPECT_FALSE(aggregator->IsNull());
        row = aggregator->Output();
        row_view.Reset(row.buf());
        EXPECT_TRUE(!row_view.IsNULL(0));
    };

    std::unique_ptr<BaseAggregator> aggregator = std::make_unique<SumAggregator<int64_t>>(agg_col_type, schema);
    check_null(aggregator.get());
    aggregator = std::make_unique<AvgAggregator<int64_t>>(agg_col_type, schema);
    check_null(aggregator.get());
    aggregator = std::make_unique<CountAggregator>(agg_col_type, schema);
    check_not_null(aggregator.get());
    aggregator = std::make_unique<MinAggregator<int64_t>>(agg_col_type, schema);
    check_null(aggregator.get());
    aggregator = std::make_unique<MaxAggregator<int64_t>>(agg_col_type, schema);
    check_null(aggregator.get());

    schema.RemoveLast();
    agg_col_type = type::kVarchar;
    column = schema.Add();
    column->set_type(agg_col_type);
    column->set_name("val");
    aggregator = std::make_unique<MinAggregator<std::string>>(agg_col_type, schema);
    check_null(aggregator.get());
    aggregator = std::make_unique<MaxAggregator<std::string>>(agg_col_type, schema);
    check_null(aggregator.get());
}

}  // namespace vm
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
