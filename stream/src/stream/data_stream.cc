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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <ctime>
#include <algorithm>
#include <fstream>
#include <vector>
#include <string>
#include <utility>
#include <sstream>
#include <iomanip>

#include <boost/algorithm/string.hpp>
#include "stream/data_stream.h"

namespace streaming {
namespace interval_join {

ElementType DataStream::GetElementType() {
    if (type_ == StreamType::kBase) {
        return ElementType::kBase;
    } else if (type_ == StreamType::kProbe) {
        return ElementType::kProbe;
    } else {
        return ElementType::kUnknown;
    }
}

int DataStream::ReadFromFile(const std::string& path, const std::string& key, const std::string& value,
                             const std::string& ts, bool parse_ts, FileType type) {
    // 1) read every line from path
    // 2) parse and create an element
    // 3) Put(element)
    if (type == FileType::kCsv) {
        std::ifstream input_file(path, std::ios_base::in);
        if (input_file.is_open()) {
            const std::string& delimiter = ",";
            std::string row;
            std::vector<std::string> col_names;
            // there must be at least one line (header line) in the csv file
            CHECK(std::getline(input_file, row));
            boost::split(col_names, row, boost::is_any_of(delimiter));
            for (auto& col_name : col_names) {
                boost::trim(col_name);
            }

            int64_t key_index, val_index, ts_index;

            auto key_it = std::find(col_names.begin(), col_names.end(), key);
            if (key_it != col_names.end()) {
                key_index = key_it - col_names.begin();
            } else {
                LOG(ERROR) << "the specified key cannot be found in csv file";
                return -1;
            }

            auto val_it = std::find(col_names.begin(), col_names.end(), value);
            if (val_it != col_names.end()) {
                val_index = val_it - col_names.begin();
            } else {
                LOG(ERROR) << "the specified value cannot be found in csv file";
                return -1;
            }

            auto ts_it = std::find(col_names.begin(), col_names.end(), ts);
            if (ts_it != col_names.end()) {
                ts_index = ts_it - col_names.begin();
            } else {
                LOG(ERROR) << "the specified ts cannot be found in csv file";
                return -1;
            }

            while (std::getline(input_file, row)) {
                std::vector<std::string> cols;
                boost::split(cols, row, boost::is_any_of(delimiter));
                std::string key_str = cols[key_index];
                boost::trim(key_str);
                std::string val_str = cols[val_index];
                boost::trim(val_str);
                int64_t timestamp = -1;
                if (parse_ts) {
                    // here we assume the time format is %Y-%m-%d %H:%M:%S.%f till microseconds (6 digits after .)
                    std::string ts_str = cols[ts_index];
                    boost::trim(ts_str);
                    std::string second_str = ts_str.substr(0, 19);
                    std::tm tm = {};
                    std::stringstream ts_stream(second_str);
                    ts_stream >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
                    std::time_t time = mktime(&tm);
                    auto seconds = static_cast<int64_t>(time);
                    timestamp = seconds * 1000000 + std::stol(ts_str.substr(20, 6));
                } else {
                    timestamp = std::stoll(cols[ts_index]);
                }
                Put(std::move(Element(key_str, val_str, timestamp, GetElementType())));
            }
            input_file.close();
        } else {
            LOG(INFO) << "cannot open input csv file";
            return -1;
        }
    } else {
        LOG(INFO) << "unsupported input file type";
        return -1;
    }
    return 0;
}

int DataStream::WriteToFile(const std::string& path, FileType type) {
    // Get and write to file
    // Get and write to file
    if (type == FileType::kCsv) {
        std::ofstream output_file(path, std::ios_base::out);
        if (output_file.is_open()) {
            output_file << "key,ts,value" << std::endl;
            auto& eles = elements();
            for (const auto& ele : eles) {
                output_file << ele->key() << "," << std::to_string(ele->ts()) << "," << ele->value() << std::endl;
            }
            output_file.close();
        } else {
            LOG(INFO) << "cannot open output csv file";
            return -1;
        }
    } else {
        LOG(INFO) << "unsupported output file type";
        return -1;
    }
    return 0;
}

}  // namespace interval_join
}  // namespace streaming
