#pragma once
#include <nlohmann/json.hpp>
#include <string>

namespace Doxybook2 {
    struct Config;

    enum class Kind {
        INDEX,
        DEFINE,
        CLASS,
        NAMESPACE,
        STRUCT,
        INTERFACE,
        FUNCTION,
        VARIABLE,
        TYPEDEF,
        USING,
        FRIEND,
        ENUM,
        ENUMVALUE,
        UNION,
        DIR,
        FILE,
        MODULE,
        PAGE,
        EXAMPLE,
        SIGNAL,
        SLOT,
        PROPERTY,
        EVENT
    };

    enum class Visibility { PUBLIC, PROTECTED, PRIVATE, PACKAGE };

    enum class Virtual { NON_VIRTUAL, VIRTUAL, PURE_VIRTUAL };

    enum class Type {
        NONE,
        DEFINES,
        FUNCTIONS,
        NAMESPACES,
        CLASSES,
        ATTRIBUTES,
        TYPES,
        DIRS,
        FILES,
        MODULES,
        FRIENDS,
        PAGES,
        EXAMPLES,
        SIGNALS,
        SLOTS,
        EVENTS,
        PROPERTIES
    };

    enum class FolderCategory { CLASSES, NAMESPACES, MODULES, PAGES, FILES, EXAMPLES };

    extern Kind toEnumKind(const std::string& str);
    extern Type toEnumType(const std::string& str);
    extern Visibility toEnumVisibility(const std::string& str);
    extern Virtual toEnumVirtual(const std::string& str);
    extern FolderCategory toEnumFolderCategory(const std::string& str);

    extern std::string toStr(Kind value);
    extern std::string toStr(Type value);
    extern std::string toStr(Visibility value);
    extern std::string toStr(Virtual value);
    extern std::string toStr(FolderCategory value);

    extern bool isKindLanguage(Kind kind);
    extern bool isKindStructured(Kind kind);
    extern bool isKindFile(Kind kind);
    extern std::string typeFolderCategoryToFolderName(const Config& config, FolderCategory type);
    extern std::string typeToFolderName(const Config& config, Type type);
    extern std::string typeToIndexName(const Config& config, FolderCategory type);
    extern std::string typeToIndexTemplate(const Config& config, FolderCategory type);
    extern std::string typeToIndexTitle(const Config& config, FolderCategory type);

    inline void to_json(nlohmann::json& j, const Visibility& p) {
        j = toStr(p);
    }

    inline void from_json(const nlohmann::json& j, Visibility& p) {
        p = toEnumVisibility(j.get<std::string>());
    }

    inline void to_json(nlohmann::json& j, const FolderCategory& p) {
        j = toStr(p);
    }

    inline void from_json(const nlohmann::json& j, FolderCategory& p) {
        p = toEnumFolderCategory(j.get<std::string>());
    }
} // namespace Doxybook2
