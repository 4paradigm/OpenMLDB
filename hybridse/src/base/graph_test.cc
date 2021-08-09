/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/graph.h"
#include <algorithm>
#include "gtest/gtest.h"

namespace hybridse {
namespace base {

class VertexNumber : public Vertex<int> {
 public:
    explicit VertexNumber(int num) : Vertex(num) {}
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
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
