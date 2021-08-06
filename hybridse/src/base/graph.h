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

#ifndef SRC_BASE_GRAPH_H_
#define SRC_BASE_GRAPH_H_
#include <iostream>
#include <unordered_map>
#include <utility>
#include <vector>
#define BOOST_ALLOW_DEPRECATED_HEADERS
#include "boost/graph/adjacency_list.hpp"
#include "boost/graph/depth_first_search.hpp"
#include "boost/graph/dijkstra_shortest_paths.hpp"
#include "boost/graph/graph_traits.hpp"
#include "glog/logging.h"
#undef BOOST_ALLOW_DEPRECATED_HEADERS

namespace hybridse {
namespace base {

template <class V>
struct Edge {
    V src;
    V dist;
};

template <class V>
class Vertex {
 public:
    explicit Vertex(V node) : node_(node) {}
    ~Vertex() {}

 protected:
    V node_;
};

class DFSVisitor : public boost::default_dfs_visitor {
 public:
    template <typename V, typename G>
    void discover_vertex(V u, const G& g) const {
        DLOG(INFO) << "At " << u;
    }
    template <typename E, typename G>
    void examine_edge(E e, const G& g) const {
        DLOG(INFO) << "Examining edges " << e;
    }
};

// Class to represent a graph
template <typename V, typename H, typename E>
class Graph {
 public:
    Graph() : num_(0) {}
    bool IsExist(const V& v);
    int FindVertex(const V& v);
    // function to add an edge to graph
    bool AddEdge(const V& v, const V& w);

    int AddVertex(const V& v);

    bool DfsVisit();
    int VertexSize();
    // prints a Topological Sort of the complete graph
 private:
 private:
    int num_;
    std::unordered_map<V, int, H, E> map;
    typedef boost::adjacency_list<boost::listS, boost::vecS, boost::directedS>
        BGraph;
    BGraph graph_;
};

template <typename V, typename H, typename E>
int Graph<V, H, E>::FindVertex(const V& v) {
    typename std::unordered_map<V, int, H, E>::iterator iter = map.find(v);
    if (iter == map.cend()) {
        return -1;
    }
    return iter->second;
}

// add vertex into grapha if vertex not exist
// return vertex id
template <typename V, typename H, typename E>
int Graph<V, H, E>::AddVertex(const V& v) {
    int id = FindVertex(v);
    if (id >= 0) {
        DLOG(INFO) << "vertex already exist! id: " << id;
        return id;
    } else {
        id = map.size();
        map.insert(std::pair<V, int>(v, id));
        DLOG(INFO) << "add vertex success! id: " << id;
        return id;
    }
}

template <typename V, typename H, typename E>
bool Graph<V, H, E>::AddEdge(const V& source, const V& target) {
    int s_id = FindVertex(source);
    if (-1 == s_id) {
        s_id = AddVertex(source);
    }

    int t_id = FindVertex(target);
    if (-1 == t_id) {
        t_id = AddVertex(target);
    }
    auto res = boost::add_edge(s_id, t_id, graph_);
    return res.second;
}

template <typename V, typename H, typename E>
bool Graph<V, H, E>::DfsVisit() {
    DFSVisitor vis;
    depth_first_search(graph_, visitor(vis));
    return true;
}
template <typename V, typename H, typename E>
bool Graph<V, H, E>::IsExist(const V& v) {
    return FindVertex(v) >= 1;
}
template <typename V, typename H, typename E>
int Graph<V, H, E>::VertexSize() {
    return map.size();
}

}  // namespace base
}  // namespace hybridse

#endif  // SRC_BASE_GRAPH_H_
