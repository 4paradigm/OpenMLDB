# OpenMLDB Online Data Exporter

OpenMLDB Online Data Exporter is used for exporting the data of a specific table to a csv file from a specific database. It can work in either standalone mode or online cluster mode.

## Requirements

- CentOS 7 / macOS >= 10.15
- net-tools
- openssh-server
- Setup SSH trust between the execution machine and the OpenMLDB deploy machines. See the link below for details:
  https://www.ssh.com/academy/ssh/sshd

## Run

 1. Compile
    ```bash
    make
    ```
    
2. Run Data Exporter
    ```
    cd /build/bin
    ./data_exporter  --db_name=<database_name> --table_name=<table_name> --config_path=<config_path> --user_name=<user_name> --delimiter=<delimiter>
    ```

To run the data exporter, users must specify the db_name, table_name and config_path in the command line.  
Users may specify the user name of deploy machines by setting the optional flag user_name. The default user_name is an empty string.  
The default delimiter for generating csv files is ',', users may change it by setting the optional flag delimiter.  

The config path could be either a relative path or an absolute path. The config file should be a yaml file.  
The result csvs would be stored at the current working directory.  

An example of the config file:

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
