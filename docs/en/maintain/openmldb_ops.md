# Operation Tool

## Overview

OpenMLDB provides an operation tool for user to use & maintain OpenMLDB conveniently. It contains the following ability:

- Recover data automatically if OpenMLDB is unavailable
- Balance partitions when scale-out
- Migrate partitions from specified endpoints to others when scale-in

## Usage

```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=scaleout
```

### Parameter Description

- --openmldb_bin_path: specify openmldb binary path
- --zk_cluster: zookeeper address
- --zk_root_path: zookeeper path
- --cmd: the operation to execute. Only support `recoverdata`, `scaleout` and `scalein` commands currently.
	- recoverdata: recover data if OpenMLDB is unavailable
	- scaleout: rebalance partitions if new endpoint added
	- scalein: migrate partitions from specified endpoints to others
- --endpoints: specified the endpoints to migrate out. If there are two or more endoints, use `,` as delimiter. It will execute failed if the leftover tablet number less than replica number of tables

**Note**:
- python >= 3.6
- Operation tool is reside in the tool directory of OpenMLDB install package.
