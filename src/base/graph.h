/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * graph.h
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BASE_GRAPH_H_
#define SRC_BASE_GRAPH_H_
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/depth_first_search.hpp>
#include <boost/graph/dijkstra_shortest_paths.hpp>
#include <boost/graph/graph_traits.hpp>
#include <iostream>
#include <unordered_map>
#include <vector>
#include "glog/logging.h"

namespace fesql {
namespace base {

template <class V>
struct Edge {
    V src;
    V dist;
};

template <class V>
class Vertex {
 public:
    Vertex(V node) : node_(node) {}
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
    bool IsExist(V& v);
    int FindVertex(V& v);
    // function to add an edge to graph
    bool AddEdge(V& v, V& w);

    int AddVertex(V& v);

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
int Graph<V, H, E>::FindVertex(V& v) {
    typename std::unordered_map<V, int, H, E>::iterator iter = map.find(v);
    if (iter == map.cend()) {
        return -1;
    }
    return iter->second;
}

// add vertex into grapha if vertex not exist
// return vertex id
template <typename V, typename H, typename E>
int Graph<V, H, E>::AddVertex(V& v) {
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
bool Graph<V, H, E>::AddEdge(V& source, V& target) {
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
bool Graph<V, H, E>::IsExist(V& v) {
    return FindVertex(v) >= 1;
}
template <typename V, typename H, typename E>
int Graph<V, H, E>::VertexSize() {
    return map.size();
}

}  // namespace base
}  // namespace fesql

#endif  // SRC_BASE_GRAPH_H_
