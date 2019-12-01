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
    for (auto rowIterator = table.rows().begin();
         rowIterator != table.rows().end(); ++rowIterator) {
        TextTable::Row const& row = *rowIterator;
        stream << table.vertical();
        for (unsigned i = 0; i < row.size(); ++i) {
            auto alignment = table.alignment(i) == TextTable::Alignment::LEFT
                                 ? std::left
                                 : std::right;
            stream << std::setw(table.width(i)) << alignment << " " + row[i];
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

void base::TextTable::determineWidths() const {
    _width.assign(columns(), 0);
    for (auto rowIterator = _rows.begin(); rowIterator != _rows.end();
         ++rowIterator) {
        Row const& row = *rowIterator;
        for (unsigned i = 0; i < row.size(); ++i) {
            _width[i] = _width[i] > row[i].size() ? _width[i] : row[i].size();
        }
    }
    for (unsigned j = 0; j < _width.size(); ++j) {
        _width[j] += 2;
    }
}

void base::TextTable::setupAlignment() const {
    for (unsigned i = 0; i < columns(); ++i) {
        if (_alignment.find(i) == _alignment.end()) {
            _alignment[i] = Alignment::LEFT;
        }
    }
}

std::string TextTable::ruler() const {
    std::string result;
    result += _corner;
    for (auto width = _width.begin(); width != _width.end(); ++width) {
        result += repeat(*width, _horizontal);
        result += _corner;
    }

    return result;
}

}  // namespace base
}  // namespace fesql
