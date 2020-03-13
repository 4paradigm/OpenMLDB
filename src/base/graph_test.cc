/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * graph_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/
#include "base/graph.h"
#include <algorithm>
#include "gtest/gtest.h"

namespace fesql {
namespace base {

class VertexNumber : public Vertex<int> {
 public:
    VertexNumber(int num) : Vertex(num) {}
    ~VertexNumber() {}
    const size_t Hash() const { return static_cast<size_t>(node_ % 10); }
    const bool Equals(const VertexNumber& that) const {
        return node_ == that.node_;
    }
};

// 1 define the hash function
struct HashVertexNumber {
    size_t operator()(const class VertexNumber& v) const {
        //  return  hash<int>(classA.getvalue());
        return v.Hash();
    }
};

// 2 define the equal function
struct EqualVertexNumber {
    bool operator()(const class VertexNumber& a1,
                    const class VertexNumber& a2) const {
        return a1.Equals(a2);
    }
};

class GraphTest : public ::testing::Test {
 public:
    GraphTest() {}
    ~GraphTest() {}
};

TEST_F(GraphTest, GraphOpTest) {
    Graph<VertexNumber, HashVertexNumber, EqualVertexNumber> g;
    VertexNumber v1(1);
    VertexNumber v2(2);
    VertexNumber v3(3);
    VertexNumber v4(4);
    VertexNumber v5(5);
    VertexNumber v6(6);
    g.AddEdge(v1, v2);
    g.AddEdge(v2, v3);
    g.AddEdge(v2, v4);
    g.AddEdge(v3, v5);
    g.AddEdge(v5, v6);
    g.DfsVisit();
}

}  // namespace base
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
