#pragma once
#include <nlohmann/json.hpp>
#include "TextPrinter.hpp"
#include "Node.hpp"
#include "Config.hpp"

namespace Doxybook2 {
    class JsonConverter {
    public:
        explicit JsonConverter(const Config& config,
                               const Doxygen& doxygen,
                               const TextPrinter& plainPrinter,
                               const TextPrinter& markdownPrinter);

        nlohmann::json convert(const std::vector<std::string>& vec) const;
        nlohmann::json convert(const Node::ClassReference& klasse) const;
        nlohmann::json convert(const Node::ClassReferences& klasses) const;
        nlohmann::json convert(const Node::Location& location) const;
        nlohmann::json convert(const Node::Param& param) const;
        nlohmann::json convert(const Node::ParameterListItem& parameterItem) const;
        nlohmann::json convert(const Node::ParameterList& parameterList) const;
        nlohmann::json convert(const Node& node) const;
        nlohmann::json convert(const Node& node, const Node::Data& data) const;
        nlohmann::json getAsJson(const Node& node) const;
    private:
        const Config& config;
        const Doxygen& doxygen;
        const TextPrinter& plainPrinter;
        const TextPrinter& markdownPrinter;
    };
}
