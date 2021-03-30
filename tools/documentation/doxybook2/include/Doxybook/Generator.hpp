#pragma once
#include "JsonConverter.hpp"
#include "Renderer.hpp"
#include <string>
#include <unordered_set>

namespace Doxybook2 {
    class Generator {
    public:
        typedef std::unordered_set<Kind> Filter;

        struct SummarySection {
            FolderCategory type;
            Filter filter;
            Filter skip;
        };

        explicit Generator(const Config& config,
            const JsonConverter& jsonConverter,
            const std::optional<std::string>& templatesPath);

        void print(const Doxygen& doxygen, const Filter& filter, const Filter& skip);
        void json(const Doxygen& doxygen, const Filter& filter, const Filter& skip);
        void manifest(const Doxygen& doxygen);
        void printIndex(const Doxygen& doxygen, FolderCategory type, const Filter& filter, const Filter& skip);
        void summary(const Doxygen& doxygen,
            const std::string& inputFile,
            const std::string& outputFile,
            const std::vector<SummarySection>& sections);

    private:
        void printRecursively(const Node& parent, const Filter& filter, const Filter& skip);
        nlohmann::json manifestRecursively(const Node& node);
        void jsonRecursively(const Node& parent, const Filter& filter, const Filter& skip);
        std::string kindToTemplateName(Kind kind);
        nlohmann::json buildIndexRecursively(const Node& node, const Filter& filter, const Filter& skip);
        void summaryRecursive(std::stringstream& ss,
            int indent,
            const std::string& folderName,
            const Node& node,
            const Filter& filter,
            const Filter& skip);
        bool shouldInclude(const Node& node);

        const Config& config;
        const JsonConverter& jsonConverter;
        Renderer renderer;
    };
} // namespace Doxybook2
