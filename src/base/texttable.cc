/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * texttable.cc
 *
 * Author: chenjing
 * Date: 2019/11/15
 *--------------------------------------------------------------------------
 **/
#include "base/texttable.h"

namespace fesql {
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
        stream << table.vertical();
        for (unsigned i = 0; i < row.size(); ++i) {
            stream << std::setw(table.width(i)) << std::left << " " + row[i];
            stream << table.vertical();
        }
        stream << "\n";
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
            widths[i] = widths[i] > row[i].size() ? widths[i] : row[i].size();
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
}  // namespace fesql
