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

#ifndef HYBRIDSE_INCLUDE_BASE_TEXTTABLE_H_
#define HYBRIDSE_INCLUDE_BASE_TEXTTABLE_H_
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace hybridse {
namespace base {
class TextTable {
 public:
    typedef std::vector<std::string> Row;
    explicit TextTable(char horizontal = '-', char vertical = '|', char corner = '+', bool middle_ruler = false)
        : horizontal_(horizontal), vertical_(vertical), corner_(corner), middle_ruler_(middle_ruler) {}

    char vertical() const { return vertical_; }

    char horizontal() const { return horizontal_; }

    void add(std::string const& content) { current_row.push_back(content); }
    size_t current_columns_size() const { return current_row.size(); }
    void end_of_row() {
        rows_.push_back(current_row);
        current_row.assign(0, "");
    }
    std::vector<Row> const& rows() const { return rows_; }
    void setup() const {
        if (rows().size() == 0) {
            return;
        }
        setup_widths();
    }

    std::string ruler() const;
    int width(unsigned i) const { return widths[i]; }
    friend std::ostream& operator<<(std::ostream& stream,
                                    const TextTable& table);

 private:
    char horizontal_;
    char vertical_;
    char corner_;
    bool middle_ruler_ = false;
    Row current_row;
    std::vector<Row> rows_;
    std::vector<unsigned> mutable widths;

    static std::string repeat(unsigned times, char c) {
        std::string result;
        for (; times > 0; --times) result += c;

        return result;
    }

    unsigned columns() const { return rows_.empty() ? 0 : rows_[0].size(); }
    void setup_widths() const;
};

}  // namespace base
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_BASE_TEXTTABLE_H_
