/**
 * Copyright (c) 2022 4Paradigm Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {

namespace v1 {

enum FeatureSignatureType {
    kFeatureSignatureContinuous = 100,
    kFeatureSignatureDiscrete = 101,
    kFeatureSignatureBinaryLabel = 200,
    kFeatureSignatureMulticlassLabel = 201,
    kFeatureSignatureRegressionLabel = 202,
};

template <typename T>
struct Continuous {
    using Args = Tuple<int32_t, Nullable<T>>(Nullable<T>);
    void operator()(T v, bool is_null, int32_t* feature_signature, T* ret, bool* null_flag) {
        *feature_signature = kFeatureSignatureContinuous;
        *null_flag = is_null;
        if (!is_null) {
            *ret = v;
        }
    }
};

template <typename T> struct Discrete;
template <typename T> struct Discrete<std::tuple<T>> {
    using Args = Tuple<int32_t, Nullable<int64_t>>(Nullable<T>);
    using ParamType = typename DataTypeTrait<T>::CCallArgType;

    void operator()(ParamType v, bool is_null, int32_t* feature_signature, int64_t* ret, bool* null_flag) {
        *feature_signature = kFeatureSignatureDiscrete;
        if (!is_null) {
            *ret = FarmFingerprint(CCallDataTypeTrait<ParamType>::to_bytes_ref(&v));
        }
        *null_flag = is_null;
    }
};

template <typename T> struct Discrete<std::tuple<T, int32_t>> {
    using Args = Tuple<int32_t, Nullable<int64_t>, int64_t>(Nullable<T>, Nullable<int32_t>);
    using ParamType = typename DataTypeTrait<T>::CCallArgType;

    void operator()(ParamType v, bool is_null, int32_t bucket_size, bool bucket_null,
            int32_t* feature_signature, int64_t* ret, bool* null_flag, int64_t* ret_bucket_size) {
        Discrete<std::tuple<T, int64_t>>()(v, is_null, bucket_size, bucket_null,
              feature_signature, ret, null_flag, ret_bucket_size);
    }
};

template <typename T> struct Discrete<std::tuple<T, int64_t>> {
    using Args = Tuple<int32_t, Nullable<int64_t>, int64_t>(Nullable<T>, Nullable<int64_t>);
    using ParamType = typename DataTypeTrait<T>::CCallArgType;

    void operator()(ParamType v, bool is_null, int64_t bucket_size, bool bucket_null,
            int32_t* feature_signature, int64_t* ret, bool* null_flag, int64_t* bucket_size_ret) {
        *feature_signature = kFeatureSignatureDiscrete;
        if (!bucket_null && bucket_size > 0) {
            if (!is_null) {
                uint64_t hash = FarmFingerprint(CCallDataTypeTrait<ParamType>::to_bytes_ref(&v));
                *ret = hash % static_cast<uint64_t>(bucket_size);
            }
            *null_flag = is_null;
            *bucket_size_ret = bucket_size;
        } else {
            *ret = 0;
            *null_flag = true;
            *bucket_size_ret = 0;
        }
    }
};

template <typename T>
struct BinaryLabel {
    using Args = Tuple<int32_t, Nullable<T>>(Nullable<T>);
    void operator()(T v, bool is_null, int32_t* feature_signature, T* ret, bool* null_flag) {
        *feature_signature = kFeatureSignatureBinaryLabel;
        *null_flag = is_null;
        if (!is_null) {
            *ret = v;
        }
    }
};


template <typename T>
struct MulticlassLabel {
    using Args = Tuple<int32_t, Nullable<T>>(Nullable<T>);
    void operator()(T v, bool is_null, int32_t* feature_signature, T* ret, bool* null_flag) {
        *feature_signature = kFeatureSignatureMulticlassLabel;
        *null_flag = is_null;
        if (!is_null) {
            *ret = v;
        }
    }
};


template <typename T>
struct RegressionLabel {
    using Args = Tuple<int32_t, Nullable<T>>(Nullable<T>);
    void operator()(T v, bool is_null, int32_t* feature_signature, T* ret, bool* null_flag) {
        *feature_signature = kFeatureSignatureRegressionLabel;
        *null_flag = is_null;
        if (!is_null) {
            *ret = v;
        }
    }
};

template<class InstanceFormat>
struct InstanceFormatHelper {
    static void Init(InstanceFormat* formatting) {
        new (formatting) InstanceFormat();
    }

    template<typename T>
    static InstanceFormat* Update(InstanceFormat* formatting, int32_t feature_signature,
          typename DataTypeTrait<T>::CCallArgType input, bool is_null) {
        formatting->template Update<T>(feature_signature, input, is_null);
        return formatting;
    }

    static InstanceFormat* UpdateDiscrete(InstanceFormat* formatting, int32_t feature_signature,
            int64_t input, bool is_null, int64_t bucket_size) {
        formatting->UpdateDiscrete(feature_signature, input, is_null, bucket_size);
        return formatting;
    }

    static void Output(InstanceFormat* formatting, StringRef* output) {
        std::string instance = formatting->Output();
        char* buffer = udf::v1::AllocManagedStringBuf(instance.size() + 1);
        if (buffer == nullptr) {
            output->size_ = 0;
            output->data_ = "";
        } else {
            memcpy(buffer, instance.c_str(), instance.size());
            buffer[instance.size()] = '\0';
            output->size_ = instance.size();
            output->data_ = buffer;
        }
        formatting->~InstanceFormat();
    }

    static void Register(UdfLibrary* library, const std::string& name, const std::string& doc) {
        library->RegisterVariadicUdf<Opaque<InstanceFormat>>(name).doc(doc)
              .init(Init)
              .template update<Tuple<int32_t, Nullable<bool>>>(Update<bool>)
              .template update<Tuple<int32_t, Nullable<int16_t>>>(Update<int16_t>)
              .template update<Tuple<int32_t, Nullable<int32_t>>>(Update<int32_t>)
              .template update<Tuple<int32_t, Nullable<int64_t>>>(Update<int64_t>)
              .template update<Tuple<int32_t, Nullable<float>>>(Update<float>)
              .template update<Tuple<int32_t, Nullable<double>>>(Update<double>)
              .template update<Tuple<int32_t, Nullable<int64_t>, int64_t>>(UpdateDiscrete)
              .output(Output);
    }
};

template<typename T>
std::string format_continuous(T value) {
    return std::to_string(value);
}

std::string format_discrete(int64_t value) {
    return std::to_string(value);
}

std::string format_binary_label(bool value) {
    return std::to_string(value);
}

std::string format_multiclass_label(int64_t value) {
    return std::to_string(value);
}

template<typename T>
std::string format_regression_label(T value) {
    return format_continuous(value);
}

struct GCFormat {
    template<class T>
    void Update(int32_t feature_signature, T input, bool is_null) {
        switch (feature_signature) {
            case kFeatureSignatureContinuous: {
                if (!is_null) {
                    if (!instance_feature.empty()) {
                        instance_feature += " ";
                    }
                    int64_t hash = FarmFingerprint(CCallDataTypeTrait<int64_t>::to_bytes_ref(&slot_number));
                    instance_feature += std::to_string(slot_number) + ":";
                    // continuous features in same slot: (hash + 0), (hash + 1), (hash + 2) ...
                    instance_feature += format_discrete(hash + 0);
                    instance_feature += ":" + format_continuous(input);
                }
                ++slot_number;
                break;
            }
            case kFeatureSignatureDiscrete: {
                if (!is_null) {
                    if (!instance_feature.empty()) {
                        instance_feature += " ";
                    }
                    instance_feature += std::to_string(slot_number) + ":" + format_discrete(input);
                }
                ++slot_number;
                break;
            }
            case kFeatureSignatureBinaryLabel: {
                instance_label = "";
                if (!is_null) {
                    instance_label = format_binary_label(input);
                }
                break;
            }
            case kFeatureSignatureMulticlassLabel: {
                instance_label = "";
                if (!is_null) {
                    instance_label = format_multiclass_label(input);
                }
                break;
            }
            case kFeatureSignatureRegressionLabel: {
                instance_label = "";
                if (!is_null) {
                    instance_label = format_regression_label(input);
                }
                break;
            }
            default: {
                ++slot_number;
                break;
            }
        }
    }

    void UpdateDiscrete(int32_t feature_signature, int64_t input, bool is_null, int64_t) {
        return Update<int64_t>(feature_signature, input, is_null);
    }

    std::string Output() {
        return instance_label + " | " + instance_feature;
    }

    size_t slot_number = 1;
    std::string instance_label;
    std::string instance_feature;
};

struct CSV {
    template<class T>
    void Update(int32_t feature_signature, T input, bool is_null) {
        if (slot_number > 1) {
            instance += ",";
        }
        switch (feature_signature) {
            case kFeatureSignatureContinuous: {
                if (!is_null) {
                    instance += format_continuous(input);
                }
                break;
            }
            case kFeatureSignatureDiscrete: {
                if (!is_null) {
                    instance += format_discrete(input);
                }
                break;
            }
            case kFeatureSignatureBinaryLabel: {
                if (!is_null) {
                    instance += format_binary_label(input);
                }
                break;
            }
            case kFeatureSignatureMulticlassLabel: {
                if (!is_null) {
                    instance += format_multiclass_label(input);
                }
                break;
            }
            case kFeatureSignatureRegressionLabel: {
                if (!is_null) {
                    instance += format_regression_label(input);
                }
                break;
            }
            default: {
                break;
            }
        }
        ++slot_number;
    }

    void UpdateDiscrete(int32_t feature_signature, int64_t input, bool is_null, int64_t) {
        return Update<int64_t>(feature_signature, input, is_null);
    }

    std::string Output() {
        return instance;
    }

    size_t slot_number = 1;
    std::string instance = "";
};

struct LIBSVM {
    template<class T>
    void Update(int32_t feature_signature, T input, bool is_null) {
        switch (feature_signature) {
            case kFeatureSignatureContinuous: {
                if (!is_null) {
                    if (!instance_feature.empty()) {
                        instance_feature += " ";
                    }
                    instance_feature += std::to_string(slot_number) + ":" + format_continuous(input);
                }
                ++slot_number;
                break;
            }
            case kFeatureSignatureDiscrete: {
                if (!is_null) {
                    if (!instance_feature.empty()) {
                        instance_feature += " ";
                    }
                    instance_feature += format_discrete(input) + ":1";
                }
                break;
            }
            case kFeatureSignatureBinaryLabel: {
                instance_label = "";
                if (!is_null) {
                    instance_label = format_binary_label(input);
                }
                break;
            }
            case kFeatureSignatureMulticlassLabel: {
                instance_label = "";
                if (!is_null) {
                    instance_label = format_multiclass_label(input);
                }
                break;
            }
            case kFeatureSignatureRegressionLabel: {
                instance_label = "";
                if (!is_null) {
                    instance_label = format_regression_label(input);
                }
                break;
            }
            default: {
                break;
            }
        }
    }

    void UpdateDiscrete(int32_t feature_signature, int64_t input, bool is_null, int64_t bucket_size) {
        if (bucket_size == 0) {
            Update<int64_t>(feature_signature, input, is_null);
        } else {
            switch (feature_signature) {
                case kFeatureSignatureDiscrete: {
                    if (!is_null) {
                        if (!instance_feature.empty()) {
                            instance_feature += " ";
                        }
                        instance_feature += format_discrete(slot_number + input) + ":1";
                    }
                    slot_number += bucket_size;
                    break;
                }
                default: {
                    break;
                }
            }
        }
    }

    std::string Output() {
        if (!instance_label.empty()) {
            if (!instance_feature.empty()) {
                return instance_label + " " + instance_feature;
            }
            return instance_label;
        } else {
            return instance_feature;
        }
    }

    size_t slot_number = 1;
    std::string instance_label;
    std::string instance_feature;
};

}  // namespace v1

void DefaultUdfLibrary::InitFeatureSignature() {
    RegisterExternalTemplate<v1::Continuous>("continuous")
        .doc(R"(
             @brief Set the column signature to continuous feature.
             Example:
             @code{.sql}
                select csv(continuous(1.5));
                -- output 1.500000
             @endcode

             @since 0.9.0
        )")
        .args_in<bool, int16_t, int32_t, int64_t, float, double>();

    RegisterExternalTemplate<v1::Discrete>("discrete")
        .doc(R"(
             @brief Set the column signature to discrete feature.
             @param input Input column
             @param bucket_size (Optional) The result is within [0, bucket_size)
             Example:
             @code{.sql}
                select csv(discrete(3), discrete(3, 100));
                -- output 2681491882390849628,28
             @endcode

             @since 0.9.0
        )")
        .args_in<std::tuple<bool>, std::tuple<bool, int32_t>, std::tuple<bool, int64_t>,
                 std::tuple<int16_t>, std::tuple<int16_t, int32_t>, std::tuple<int16_t, int64_t>,
                 std::tuple<int32_t>, std::tuple<int32_t, int32_t>, std::tuple<int32_t, int64_t>,
                 std::tuple<int64_t>, std::tuple<int64_t, int32_t>, std::tuple<int64_t, int64_t>,
                 std::tuple<StringRef>, std::tuple<StringRef, int32_t>, std::tuple<StringRef, int64_t>,
                 std::tuple<Timestamp>, std::tuple<Timestamp, int32_t>, std::tuple<Timestamp, int64_t>,
                 std::tuple<Date>, std::tuple<Date, int32_t>, std::tuple<Date, int64_t> >();

    RegisterExternalTemplate<v1::BinaryLabel>("binary_label")
        .doc(R"(
             @brief Set the column signature to binary label.
             Example:
             @code{.sql}
                select csv(binary_label(true));
                -- output 1
             @endcode

             @since 0.9.0
        )")
        .args_in<bool>();

    RegisterExternalTemplate<v1::MulticlassLabel>("multiclass_label")
        .doc(R"(
             @brief Set the column signature to multiclass label.
             Example:
             @code{.sql}
                select csv(multiclass_label(6));
                -- output 6
             @endcode

             @since 0.9.0
        )")
        .args_in<bool, int16_t, int32_t, int64_t>();

    RegisterExternalTemplate<v1::RegressionLabel>("regression_label")
        .doc(R"(
             @brief Set the column signature to regression label.
             Example:
             @code{.sql}
                select csv(regression_label(1.5));
                -- output 1.500000
             @endcode

             @since 0.9.0
        )")
        .args_in<bool, int16_t, int32_t, int64_t, float, double>();

    v1::InstanceFormatHelper<v1::GCFormat>::Register(this, "gcformat", R"(
        @brief Return instance in GCFormat format.
             Example:
             @code{.sql}
                select gcformat(multiclass_label(6), continuous(1.5), category(3));
                -- output 6 | 1:0:1.500000 2:2681491882390849628
             @endcode

             @since 0.9.0
        )");

    v1::InstanceFormatHelper<v1::CSV>::Register(this, "csv", R"(
        @brief Return instance in CSV format.
             Example:
             @code{.sql}
                select csv(multiclass_label(6), continuous(1.5), category(3));
                -- output 6,1.500000,2681491882390849628
             @endcode

             @since 0.9.0
        )");

    v1::InstanceFormatHelper<v1::LIBSVM>::Register(this, "libsvm", R"(
        @brief Return instance in LIBSVM format.
             Example:
             @code{.sql}
                select libsvm(multiclass_label(6), continuous(1.5), category(3));
                -- output 6 1:1.500000 2681491882390849628:1
             @endcode

             @since 0.9.0
        )");
}

}  // namespace udf
}  // namespace hybridse
