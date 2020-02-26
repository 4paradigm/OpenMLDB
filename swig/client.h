#include "client/ns_client.h"
#include "zk/zk_client.h"
#include <string>

rtidb::client::NsClient* InitNsClient(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint);
std::vector<std::string>* ShowTable(rtidb::client::NsClient* client, const std::string& name);
