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

#include "base/fe_strings.h"
#include "base/texttable.h"

namespace hybridse {
namespace base {
std::ostream& operator<<(std::ostream& stream, const TextTable& table) {
    if (0 == table.rows().size()) {
        return stream;
    }
    table.setup();
    stream << table.ruler() << "\n";
    unsigned line = 0;
    for (auto row_iterator = table.rows().begin();
         row_iterator != table.rows().end(); ++row_iterator) {
        TextTable::Row const& row = *row_iterator;
        std::vector<std::vector<std::string>> rows;
        size_t max_lines = 0;
        for (unsigned i = 0; i < row.size(); ++i) {
            std::vector<std::string> toks;
            SplitString(row[i], "\n", toks);
            rows.emplace_back(toks);
            max_lines = std::max(max_lines, toks.size());
        }
        std::vector<std::stringstream> lines(max_lines);
        for (unsigned j = 0; j < max_lines; j++) {
            lines[j] << table.vertical();
            for (unsigned i = 0; i < row.size(); ++i) {
                lines[j] << std::setw(table.width(i)) << std::left << " " + (rows[i].size() > j ? rows[i][j] : "")
                         << table.vertical();
            }
            lines[j] << "\n";
        }
        for (unsigned j = 0; j < max_lines; j++) {
            stream << lines[j].str();
        }
        if (line < 1 || line == table.rows().size() - 1) {
            stream << table.ruler() << "\n";
        }
        line++;
    }

    return stream;
}

void base::TextTable::setup_widths() const {
    widths.assign(columns(), 0);
    for (auto rowIterator = rows_.begin(); rowIterator != rows_.end();
         ++rowIterator) {
        Row const& row = *rowIterator;
        for (unsigned i = 0; i < row.size(); ++i) {
            std::vector<std::string> toks;
            SplitString(row[i], "\n", toks);
            size_t max_size = 0;
            for (unsigned j = 0; j < toks.size(); j++) {
                max_size = std::max(max_size, toks[j].size());
            }
            widths[i] = widths[i] > max_size ? widths[i] : max_size;
        }
    }
    for (unsigned j = 0; j < widths.size(); ++j) {
        widths[j] += 2;
    }
}

std::string TextTable::ruler() const {
    std::string result;
    result += corner_;
    for (auto width = widths.begin(); width != widths.end(); ++width) {
        result += repeat(*width, horizontal_);
        result += corner_;
    }

    return result;
}

}  // namespace base
}  // namespace hybridse
