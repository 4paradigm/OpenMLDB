#pragma once
#include "TextPrinter.hpp"

namespace Doxybook2 {
    class TextPlainPrinter : public TextPrinter {
    public:
        explicit TextPlainPrinter(const Config& config, const Doxygen& doxygen)
            : TextPrinter(config, doxygen) {

        }

        std::string print(const XmlTextParser::Node& node) const override;
    private:
        void print(std::stringstream& ss, const XmlTextParser::Node& node) const;
    };
}
