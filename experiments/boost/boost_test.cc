/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * boost_test.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/depth_first_search.hpp>
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/topological_sort.hpp>
#include <utility>
#include "gtest/gtest.h"

namespace fesql {
namespace base {

class BoostTest : public ::testing::Test {
 public:
    BoostTest() {}
    ~BoostTest() {}
};

using namespace std;
using namespace boost;
typedef property<edge_weight_t, int> EdgeWeightProperty;
typedef boost::adjacency_list<listS, vecS, undirectedS, no_property,
                              EdgeWeightProperty>
    mygraph;
class custom_dfs_visitor : public boost::default_dfs_visitor {
 public:
    template <typename Vertex, typename Graph>
    void discover_vertex(Vertex u, const Graph& g) const {
        std::cout << "At " << u << std::endl;
    }
    template <typename Edge, typename Graph>
    void examine_edge(Edge e, const Graph& g) const {
        std::cout << "Examining edges " << e << std::endl;
    }
};

TEST_F(BoostTest, graph_dfs_test) {
    typedef adjacency_list<listS, vecS, directedS> Graph;
    Graph g;
    // 2.增加节点和边方法
    add_edge(0, 1, g);
    add_edge(1, 4, g);
    add_edge(4, 3, g);
    add_edge(4, 5, g);
    add_edge(3, 6, g);
    add_edge(6, 2, g);
    custom_dfs_visitor vis;
    depth_first_search(g, visitor(vis));
}
TEST_F(BoostTest, simple_graph_test) {
    using namespace boost;
    using namespace std;
    ASSERT_TRUE(true);
    // 1.新建无向图，使用邻接矩阵数据结构
    typedef adjacency_list<listS, vecS, directedS> Graph;
    Graph g;
    // 2.增加节点和边方法
    add_edge(0, 1, g);
    add_edge(1, 4, g);
    add_edge(4, 0, g);
    add_edge(2, 5, g);
    add_edge(2, 6, g);
    // 3.遍历点
    typedef graph_traits<Graph>::vertex_iterator vertex_iter;
    pair<vertex_iter, vertex_iter> vip;
    cout << "Vertices in g  = [ ";
    vip = vertices(g);
    vector<int> vec;
    for (vertex_iter vi = vip.first; vi != vip.second; ++vi) {
        cout << *vi << " ";
        vec.push_back(*vi);
    }
    cout << "]" << endl;
    ASSERT_EQ(vec, vector<int>({0, 1, 2, 3, 4, 5, 6}));

    // 4.遍历边方法1
    vector<pair<int, int>> edge_vec;
    typedef graph_traits<Graph>::edge_iterator edge_iter;
    pair<edge_iter, edge_iter> eip;
    eip = edges(g);
    cout << "Edge in g  = [ ";
    for (edge_iter ei = eip.first; ei != eip.second; ++ei) {
        // cout << *ei << " ";
        cout << "( source edge=" << source(*ei, g);
        cout << " taget edge=" << target(*ei, g) << ")" << endl;
        edge_vec.push_back(
            std::make_pair<int, int>(source(*ei, g), target(*ei, g)));
    }
    cout << "]" << endl;
    vector<pair<int, int>> exp_vec;
    exp_vec.push_back(make_pair(0, 1));
    exp_vec.push_back(make_pair(1, 4));
    exp_vec.push_back(make_pair(2, 5));
    exp_vec.push_back(make_pair(2, 6));
    exp_vec.push_back(make_pair(4, 0));
    ASSERT_EQ(edge_vec, exp_vec);
}

}  // namespace base
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}