# Operation Tool

## Overview

OpenMLDB provides an operation tool for user to use & maintain OpenMLDB conveniently. It contains the following ability:

- Recover data automatically: `recoverdata`
- Balance partitions when scale-out: `scaleout`
- Migrate partitions from specified endpoints to others when scale-in: `scalein`
- Pre-upgrade and post-upgrade for tablet upgrade: `pre-upgrade` and `post-upgrade`
- Check op status: `showopstatus`
- Check table status: `showtablestatus`

## Usage
### Common parameters
- --openmldb_bin_path: specify openmldb binary path
- --zk_cluster: zookeeper address
- --zk_root_path: zookeeper path
- --cmd: the operation to execute

### Operations
| Operation         | Function                                                                                                                                                                                                                                                                                                             | Extra Parameters                                                                                                                                                                        |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `recoverdata`     | recover all tables                                                                                                                                                                                                                                                                                                   | -                                                                                                                                                                                       |
| `scaleout`        | rebalance partitions among tablets                                                                                                                                                                                                                                                                                   | -                                                                                                                                                                                       |
| `scalein`         | migrate partitions from specified endpoints to others. If the number of remaining tablets is smaller than the max replicanum, it will fail                                                                                                                                                                           | --endpoints: the endpoints that are migrated from, multiple endpoints are delimited by `,`                                                                                              |
| `pre-upgrade`     | tablet pre-upgrade, which will migrate all the leader partitions to other tablets to reduce the influence on the online service. If there is only one replica for a partition, we will create an extra one which will be deleted automatically during `post-upgrade`. All the changes will be recorded in `statfile` | --endpoints: the endpoints that are upgraded, only one endpoint is allowed <br/> --statfile：the temporary file to record the leader change information, which defaults to `.stat`       |
| `post-upgrade`    | tablet post-upgrade, which will revert all the changes recorded in `statfile`                                                                                                                                                                                                                                        | --endpoints: the endpoints that are upgraded, only one endpoint is allowed <br/> --statfile：the temporary file to record the leader change information, which defaults to `.stat`       |
| `showopstatus`    | check the op status                                                                                                                                                                                                                                                                                                  | --filter: only show the records with status `filter`. Valid statuses are `kInited`, `kDoing`, `kDone`, `kFailed` and `kCanceled`.Its default is `None`, meaning showing all the records |
| `showtablestatus` | check the table status                                                                                                                                                                                                                                                                                               | --filter: the required database pattern. Its matching rule is the same as `LIKE` op. Its default is `'%'`，meaning showing the tables under all the databases                            |

### Example
```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=scaleout
```

### Parameter Description

- --endpoints: specified the endpoints to migrate out. If there are two or more endoints, use `,` as delimiter. It will execute failed if the leftover tablet number less than replica number of tables

### System Requirements
- python >= 3.6
- `showopstatus` and `showtablestatus` require `prettytable` dependency