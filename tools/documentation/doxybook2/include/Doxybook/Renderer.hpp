#pragma once
#include "Config.hpp"
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <unordered_map>

namespace inja {
    struct Template;
    class Environment;
} // namespace inja

namespace Doxybook2 {
    class Renderer {
    public:
        explicit Renderer(const Config& config, const std::optional<std::string>& templatesPath = std::nullopt);
        ~Renderer();

        void render(const std::string& name, const std::string& path, const nlohmann::json& data) const;
        std::string render(const std::string& name, const nlohmann::json& data) const;

    private:
        const Config& config;

        std::unique_ptr<inja::Environment> env;
        std::unordered_map<std::string, std::unique_ptr<inja::Template>> templates;
    };
} // namespace Doxybook2
