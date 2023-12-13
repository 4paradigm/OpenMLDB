# Use of Online Data Export Tools

The online data export tool is located in [src/tools](https://github.com/4paradigm/OpenMLDB/tree/main/src/tools). Support exporting data in standalone or cluster mode from remote machines.

## Installation

To generate Unix Executable file: Use make in the src directory.

## Usage

### Parameter Settings

All configurations are as follows, with an asterisk indicating mandatory items.

```
Usage: ./data_exporter [--delimiter=<delimiter>] --db_name=<dbName> 
                     [--user_name=<userName>] --table_name=<tableName>
                     --config_path=<configPath>
      
*     --db_name=<dbName>          openmldb database name
*     --table_name=<tableName>    openmldb table name of the selected database
*     --config_path=<configPath>  absolute or relative path of the config file
      --delimiter=<delimiter>     delimiter for the output csv, default is ','
      --user_name=<userName>      user name of the remote machine
```

### Important Parameter Configurations
- `--db_name=<dbName>`: Specifies the database name. The provided database name must already exist. If it does not exist, an error message will be displayed: "The database does not exist."
- `--table_name=<tableName>`: Specifies the table name. The specified table name must already exist. If it does not exist, an error message will be displayed: "The table does not exist."
- `--config_path=<configPath>`: Specifies the path to the configuration file for remote OpenMLDB machines, in YAML format.

### Configuration File Example

     mode: cluster
     zookeeper:
         zk_cluster: 172.17.0.2:2181
         zk_root_path: /openmldb
     nameserver:
     - 
         endpoint: 172.17.0.2:6527
         path: /work/ns1
     - 
         endpoint: 172.17.0.2:6528
         path: /work/ns2
     tablet:
     - 
         endpoint: 172.17.0.2:10921
         path: /work/openmldb
     - 
         endpoint: 172.17.0.2:10922
         path: /work/openmldb

## Error Handling

If the data export fails, the cause of the error can be determined based on the error information in Glog.
