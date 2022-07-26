from diagnostic_tool.collector import Collector
from diagnostic_tool.dist_conf import DistConfReader

if __name__ == '__main__':
    dist_conf = DistConfReader('../tests/cluster_dist.yml').conf()
    print(dist_conf)
    conns = Collector(dist_conf)
    # conns.ping_all()
    conns.pull_config_files('/tmp/cluster1/conf')
    conns.pull_log_files('/tmp/cluster1/logs')
    conns.collect_version()
