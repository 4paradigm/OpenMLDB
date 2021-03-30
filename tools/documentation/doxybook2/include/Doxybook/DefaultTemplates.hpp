#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace Doxybook2 {
    struct DefaultTemplate {
        std::string src;
        std::vector<std::string> dependencies;
    };

    extern std::unordered_map<std::string, DefaultTemplate> defaultTemplates;
    extern void saveDefaultTemplates(const std::string& path);
} // namespace Doxybook2