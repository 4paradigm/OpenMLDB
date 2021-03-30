#pragma once
#include <string>
#include <functional>
#include <memory>

namespace tinyxml2 {
    class XMLNode;
    class XMLElement;
    class XMLDocument;
}

namespace Doxybook2 {
    class Xml {
    public:
        class Element;

        typedef std::function<void(Element&)> ElementCallback;

        class Node {
        public:
            Node() = default;
            explicit Node(tinyxml2::XMLNode* ptr);
            ~Node() = default;

            Node nextSibling() const;
            Node firstChild() const;

            bool hasText() const;
            std::string getText() const;

            bool isElement() const;
            Element asElement() const;

            operator bool() const {
                return ptr != nullptr;
            }

        private:
            tinyxml2::XMLNode* ptr{nullptr};
        };

        class Element {
        public:
            Element() = default;
            explicit Element(tinyxml2::XMLElement* ptr);
            ~Element() = default;

            void allChildElements(const std::string& name, const ElementCallback& callback) const;
            Node asNode() const;
            Element nextSiblingElement() const;
            Node nextSibling() const;
            Element nextSiblingElement(const std::string& name) const;
            Node firstChild() const;
            Element firstChildElement() const;
            Element firstChildElement(const std::string& name) const;
            int getLine() const;
            const Xml& getDocument() const;

            std::string getAttr(const std::string& name) const;
            std::string getAttr(const std::string& name, const std::string& defaultValue) const;
            std::string getName() const;

            bool hasText() const;
            std::string getText() const;

            operator bool() const {
                return ptr != nullptr;
            }

        private:
            tinyxml2::XMLElement* ptr{nullptr};
        };

        explicit Xml(const std::string& path);
        ~Xml();

        Element firstChildElement(const std::string& name) const;

        const std::string& getPath() const {
            return path;
        }

    private:
        std::unique_ptr<tinyxml2::XMLDocument> doc;
        std::string path;
    };
}
