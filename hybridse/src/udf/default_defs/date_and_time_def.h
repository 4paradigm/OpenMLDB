/**
 * Copyright (c) 2023 4paradigm authors
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

#ifndef HYBRIDSE_SRC_UDF_DEFAULT_DEFS_DATE_AND_TIME_DEF_H_
#define HYBRIDSE_SRC_UDF_DEFAULT_DEFS_DATE_AND_TIME_DEF_H_

#include <tuple>
#include <type_traits>

#include "base/type.h"
#include "udf/literal_traits.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {

template <typename DateType>
struct AddMonths {
    using Args = std::tuple<DateType, int32_t>;
    using CDateType = typename DataTypeTrait<DateType>::CCallArgType;

    template <typename T = DateType, std::enable_if_t<std::is_same_v<openmldb::base::Date, T>, int>* = nullptr>
    void operator()(CDateType date, int32_t num_months, openmldb::base::Date* ret, bool* is_null) {
        int32_t year = 0, month = 0, day = 0;
        if (!openmldb::base::Date::Decode(date->date_, &year, &month, &day)) {
            *is_null = true;
            return;
        }

        absl::CivilDay civil_day(year, month, day);
        if (civil_day.year() != year || civil_day.month() != month || civil_day.day() != day) {
            // absl normalize the date, we do not expect that, fail fast
            *is_null = true;
            return;
        }

        absl::CivilMonth result_month = absl::CivilMonth(year, month) + num_months;
        // last day of result month
        auto civil_last_day = absl::CivilDay(result_month + 1) - 1;
        int32_t last_day = civil_last_day.day();
        int32_t result_day = last_day < day ? last_day : day;

        *ret = openmldb::base::Date(result_month.year(), result_month.month(), result_day);
    }
};
}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_DEFAULT_DEFS_DATE_AND_TIME_DEF_H_
