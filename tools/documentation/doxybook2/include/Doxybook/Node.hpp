#pragma once
#include "Enums.hpp"
#include "Xml.hpp"
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace Doxybook2 {
    class TextPrinter;
    class Node;
    struct Config;

    typedef std::shared_ptr<Node> NodePtr;
    typedef std::unordered_map<std::string, NodePtr> NodeCacheMap;

    class Node {
      public:
        typedef std::list<NodePtr> Children;

        struct ClassReference {
            std::string name;
            std::string refid;
            Visibility prot;
            Virtual virt;
            const Node* ptr{nullptr};
        };

        struct Location {
            std::string file;
            int line{0};
            int column{0};
            std::string bodyFile;
            int bodyStart{0};
            int bodyEnd{0};
        };

        struct Param {
            std::string type;
            std::string typePlain;
            std::string name;
            std::string defval;
            std::string defvalPlain;
        };

        struct ParameterListItem {
            std::string name;
            std::string text;
        };

        typedef std::vector<ParameterListItem> ParameterList;
        typedef std::vector<ClassReference> ClassReferences;
        typedef std::vector<Param> Params;

        struct Data {
            ClassReferences baseClasses;
            std::string definition;
            std::string argsString;
            std::string initializer;
            ClassReferences derivedClasses;
            bool isAbstract{false};
            bool isStatic{false};
            bool isConst{false};
            bool isExplicit{false};
            bool isStrong{false};
            bool isInline{false};
            bool isDefault{false};
            bool isDeleted{false};
            bool isOverride{false};
            Location location;
            std::string details;
            std::string inbody;
            std::string includes;
            std::string type;
            std::string typePlain;
            std::string deprecated;
            Params params;
            Params templateParams;
            std::vector<std::string> see;
            std::vector<std::string> returns;
            std::vector<std::string> authors;
            std::vector<std::string> version;
            std::vector<std::string> since;
            std::vector<std::string> date;
            std::vector<std::string> note;
            std::vector<std::string> warning;
            std::vector<std::string> pre;
            std::vector<std::string> post;
            std::vector<std::string> copyright;
            std::vector<std::string> invariant;
            std::vector<std::string> remark;
            std::vector<std::string> attention;
            std::vector<std::string> par;
            std::vector<std::string> rcs; // What is this?
            std::vector<std::string> bugs;
            std::vector<std::string> tests;
            std::vector<std::string> todos;
            ParameterList paramList;
            ParameterList returnsList;
            ParameterList templateParamsList;
            ParameterList exceptionsList;
            const Node* reimplements{nullptr};
            std::vector<const Node*> reimplementedBy;
            std::string programlisting;
        };

        typedef std::unordered_map<std::string, Data> ChildrenData;

        // Parse root xml objects (classes, structs, etc)
        static NodePtr
        parse(NodeCacheMap& cache, const std::string& inputDir, const std::string& refid, bool isGroupOrFile);

        static NodePtr parse(NodeCacheMap& cache, const std::string& inputDir, const NodePtr& ptr, bool isGroupOrFile);

        // Parse member xml objects (functions, enums, etc)
        static NodePtr parse(Xml::Element& memberdef, const std::string& refid);

        explicit Node(const std::string& refid);
        ~Node();

        NodePtr find(const std::string& refid) const;

        NodePtr findChild(const std::string& refid) const;

        bool isStructured() const {
            return isKindStructured(kind);
        }

        bool isLanguage() const {
            return isKindLanguage(kind);
        }

        bool isFileOrDir() const {
            return isKindFile(kind);
        }

        Kind getKind() const {
            return kind;
        }

        Type getType() const {
            return type;
        }

        const std::string& getRefid() const {
            return refid;
        }

        const std::string& getName() const {
            return name;
        }

        const Node* getParent() const {
            return parent;
        }

        const Node* getGroup() const {
            return group;
        }

        bool isEmpty() const {
            return empty;
        }

        const Children& getChildren() const {
            return children;
        }

        const std::string& getXmlPath() const {
            return xmlPath;
        }

        const std::string& getBrief() const {
            return brief;
        }

        const std::string& getSummary() const {
            return summary;
        }

        const std::string& getTitle() const {
            return title;
        }

        Visibility getVisibility() const {
            return visibility;
        }

        Virtual getVirtual() const {
            return virt;
        }

        const ClassReferences& getBaseClasses() const {
            return baseClasses;
        }

        const ClassReferences& getDerivedClasses() const {
            return derivedClasses;
        }

        const std::string& getUrl() const {
            return url;
        }

        const std::string& getAnchor() const {
            return anchor;
        }

        void finalize(const Config& config,
            const TextPrinter& plainPrinter,
            const TextPrinter& markdownPrinter,
            const NodeCacheMap& cache);
        typedef std::tuple<Data, ChildrenData> LoadDataResult;
        LoadDataResult loadData(const Config& config,
            const TextPrinter& plainPrinter,
            const TextPrinter& markdownPrinter,
            const NodeCacheMap& cache) const;

        friend class Doxygen;

      private:
        class Temp;
        Data loadData(const Config& config,
            const TextPrinter& plainPrinter,
            const TextPrinter& markdownPrinter,
            const NodeCacheMap& cache,
            const Xml::Element& element) const;
        ClassReferences getAllBaseClasses(const NodeCacheMap& cache);

        std::unique_ptr<Temp> temp;
        Kind kind{Kind::INDEX};
        Type type{Type::NONE};
        std::string refid;
        std::string name;
        std::string brief;
        std::string summary;
        std::string title;
        Node* parent{nullptr};
        Node* group{nullptr};
        Children children;
        bool empty{true};
        std::string xmlPath;
        ClassReferences baseClasses;
        ClassReferences derivedClasses;
        Visibility visibility{Visibility::PUBLIC};
        Virtual virt{Virtual::NON_VIRTUAL};
        std::string url;
        std::string anchor;

        void parseBaseInfo(const Xml::Element& element);
        void parseInheritanceInfo(const Xml::Element& element);
        NodePtr findRecursively(const std::string& refid) const;
        static Xml::Element assertChild(const Xml::Element& xml, const std::string& name);
        static Xml::Element assertChild(const Xml& xml, const std::string& name);
    };
} // namespace Doxybook2
