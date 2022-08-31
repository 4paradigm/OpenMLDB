# OpenMLDB Online Data Exporter

OpenMLDB Online Data Exporter is used for exporting the data of a specific table to a csv file from a specific database. It can work in either standalone mode or online cluster mode.

## Requirements

- CentOS 7 / macOS >= 10.15
- net-tools
- openssh-server

## Run

 1. Compile
    ```bash
    make
    ```
2. Install net-tools and openssh-server. Then generate ssh keys (first time only)
    ```bash
    yum install -y net-tools
    yum install openssh-server
    ssh-keygen -A
    ```
3. Start ssd
    ```bash
    /usr/sbin/sshd
    ```
4. Run Data Exporter
    ```
    cd /build/bin
    ./data_exporter  --db_name=<database_name> --table_name=<table_name> --config_path=<config_path>
    ```

The user needs to specify the db_name, table_name and config_path in the command line. 

The config path could be either a relative path or an absolute path. The config file should be a yaml file.

The result csvs would be stored at the current working directory.
