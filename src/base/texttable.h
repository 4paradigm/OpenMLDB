/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * texttable.h.cpp
 *
 * Author: chenjing
 * Date: 2019/11/12
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_BASE_TEXTTABLE_H_
#define SRC_BASE_TEXTTABLE_H_
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace fesql {
namespace base {
class TextTable {
 public:
    enum class Alignment { LEFT, RIGHT };
    typedef std::vector<std::string> Row;
    explicit TextTable(char horizontal = '-', char vertical = '|',
                       char corner = '+')
        : _horizontal(horizontal), _vertical(vertical), _corner(corner) {}

    void setAlignment(unsigned i, Alignment alignment) {
        _alignment[i] = alignment;
    }

    Alignment alignment(unsigned i) const { return _alignment[i]; }

    char vertical() const { return _vertical; }

    char horizontal() const { return _horizontal; }

    void add(std::string const& content) { _current.push_back(content); }

    void endOfRow() {
        _rows.push_back(_current);
        _current.assign(0, "");
    }

    template <typename Iterator>
    void addRow(Iterator begin, Iterator end) {
        for (auto i = begin; i != end; ++i) {
            add(*i);
        }
        endOfRow();
    }

    template <typename Container>
    void addRow(Container const& container) {
        addRow(container.begin(), container.end());
    }

    std::vector<Row> const& rows() const { return _rows; }

    void setup() const {
        if (rows().size() == 0) {
            return;
        }
        determineWidths();
        setupAlignment();
    }

    std::string ruler() const;

    void setupAlignment() const;
    int width(unsigned i) const { return _width[i]; }
    friend std::ostream& operator<<(std::ostream& stream,
                                    const TextTable& table);

 private:
    char _horizontal;
    char _vertical;
    char _corner;
    Row _current;
    std::vector<Row> _rows;
    std::vector<unsigned> mutable _width;
    std::map<unsigned, Alignment> mutable _alignment;

    static std::string repeat(unsigned times, char c) {
        std::string result;
        for (; times > 0; --times) result += c;

        return result;
    }

    unsigned columns() const { return _rows[0].size(); }
    void determineWidths() const;
};

}  // namespace base
}  // namespace fesql
#endif  // SRC_BASE_TEXTTABLE_H_
