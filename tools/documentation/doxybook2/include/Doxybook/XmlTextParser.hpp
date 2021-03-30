#pragma once
#include "Xml.hpp"
#include <string>
#include <vector>

namespace Doxybook2 {
    class XmlTextParser {
    public:
        class Tag;
        typedef std::unique_ptr<Tag> TagPtr;
        typedef std::vector<TagPtr> Children;

        struct Node {
            enum class Type {
                UNKNOWN = -1,
                TEXT = 0,
                PARA,
                PARAS,
                BOLD,
                EMPHASIS,
                STRIKE,
                HRULER,
                IMAGE,
                ULINK,
                REF,
                COMPUTEROUTPUT,
                LISTITEM,
                SIMPLESEC,
                ITEMIZEDLIST,
                VARIABLELIST,
                ORDEREDLIST,
                PARAMETERLIST,
                PARAMETERNAME,
                PARAMETERITEM,
                PARAMETERDESCRIPTION,
                PARAMETERNAMELIST,
                XREFSECT,
                XREFTITLE,
                XREFDESCRIPTION,
                PROGRAMLISTING,
                CODELINE,
                TERM,
                VARLISTENTRY,
                ANCHOR,
                SP,
                HIGHTLIGHT,
                SECT1,
                SECT2,
                SECT3,
                SECT4,
                SECT5,
                SECT6,
                TITLE,
                SUPERSCRIPT,
                NONBREAKSPACE,
                TABLE,
                TABLE_ROW,
                TABLE_CELL,
                VERBATIM,
                SQUO,
                NDASH,
                MDASH,
                LINEBREAK,
                ONLYFOR,
                FORMULA,
            };

            Type type{Type::UNKNOWN};
            std::vector<Node> children;
            std::string data;
            std::string extra;
        };

        static Node parseParas(const Xml::Element& element);
        static Node parsePara(const Xml::Element& element);
        static Node::Type strToType(const std::string& str);

    private:
        static void traverse(std::vector<Node*> tree, const Xml::Node& element);
    };
} // namespace Doxybook2
