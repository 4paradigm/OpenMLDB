//
// Created by hezhaozhao on 2022/4/25.
//

#include "udf_json.h"

#include <regex>
#include <vector>

#include "nlohmann/json.hpp"
#include "udf/udf.h"

namespace hybridse {
using openmldb::base::StringRef;
namespace udf {
using json = nlohmann::json;
namespace json_func {

auto loads_json(const std::string &json_str) {
    if (json_str.empty()) {
        return json();
    }
    try {
        return json::parse(json_str);
    } catch (std::exception &e) {
        return json();
    }
}

std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        if (item.length() > 0) {
            elems.emplace_back(item);
        }
    }
    return elems;
}

std::vector<std::string> str_split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

json extract_json_with_key(const json &j, const std::string &key) {
    if (j.is_array()) {
        json j_new = json::array();
        for (auto &item : j) {
            if (item.is_array()) {
                for (auto &item_array : item) {
                    j_new.push_back(item_array);
                }
            } else if (item.is_object()) {
                json item_key = {};
                if (item.contains(key)) {
                    item_key = item[key];
                }
                if (!item_key.is_null()) {
                    j_new.push_back(item_key);
                }
            }
        }
        return j_new;
    } else if (j.is_object()) {
        if (j.contains(key)) {
            return j[key];
        } else {
            return {};
        }
    } else {
        return {};
    }
}

json extract_json_with_index(const json &j, std::vector<std::string> &index_list) {
    json j_new = json::array();
    for (auto &index : index_list) {
        std::transform(index.begin(), index.end(), index.begin(), ::tolower);
        if (index == "*") {
            if (j.is_array()) {
                for (auto &item : j) {
                    j_new.push_back(item);
                }
            }
        } else {
            size_t index_num = std::stoi(index);
            if (j.is_array()) {
                if (index_num < j.size()) {
                    j_new.push_back(j[index_num]);
                }
            }
        }
    }
    if (j_new.empty()) {
        return {};
    }
    return j_new;
}

json extract_json(json &json_obj, const std::string &path, bool skip_map) {
    std::regex pattern_index(R"(\[([0-9\s,]+|\*)\])");
    std::regex pattern_key(R"(^([a-zA-Z0-9_\-\:\s]+).*)");
    std::smatch base_match;
    if (!skip_map) {
        if (!std::regex_match(path, base_match, pattern_key)) {
            return {};
        }
        std::string key = base_match[1];
        json json = extract_json_with_key(json_obj, key);
        return json;

    } else {
        if (!std::regex_match(path, base_match, pattern_index)) {
            return {};
        }
        std::string index_str = base_match[1];
        index_str.erase(std::remove(index_str.begin(), index_str.end(), ' '), index_str.end());
        std::vector<std::string> index_list = str_split(index_str, ',');
        if (index_list.empty()) {
            return {};
        }
        json json = extract_json_with_index(json_obj, index_list);
        return json;
    }
}

void get_json_object(StringRef *json_string, StringRef *path_string, StringRef *output, bool *is_null_ptr) {
    *is_null_ptr = true;
    if (json_string == nullptr || json_string->size_ == 0 || path_string == nullptr || path_string->size_ == 0) {
        return;
    }
    std::string json_str = json_string->ToString();
    std::string path_str = path_string->ToString();
    if (json_str.empty() || path_str.empty()) {
        return;
    }
    if (path_str.substr(0, 1) != "$") {
        return;
    }
    size_t path_start = 1;
    bool is_root_array = false;
    if (path_str.length() > 1) {
        if (path_str.substr(1, 1) == "[") {
            is_root_array = true;
            path_start = 0;
        } else if (path_str.substr(1, 1) == ".") {
            is_root_array = path_str.length() > 2 && path_str.substr(2, 1) == "[";
        } else {
            return;
        }
    }
    json json_obj = loads_json(json_str);
    if (json_obj.is_null()) {
        return;
    }
    std::vector<std::string> path_elems = str_split(path_str, '.');
    for (size_t i = 1; i < path_elems.size(); ++i) {
        bool skip_map = is_root_array && i == path_start;
        if (path_elems[i].substr(0, 1) == "[" && path_elems[i].substr(path_elems[i].length() - 1, 1) == "]") {
            skip_map = true;
        }
        // call extract_json(json_obj, path_elems[i], skip_map); return json_obj
        json_obj = extract_json(json_obj, path_elems[i], skip_map);
    }
    if (json_obj.empty()) {
        return;
    }
    std::string output_data = json_obj.dump();
    int output_size = output_data.length();
    char *buffer = udf::v1::AllocManagedStringBuf(output_size);
    memcpy(buffer, output_data.c_str(), output_size);
    output->size_ = output_size;
    output->data_ = buffer;
    *is_null_ptr = false;
    return;
}
}  // namespace v1
}  // namespace udf
}  // namespace hybridse
