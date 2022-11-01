
# Code Structure

## Hybridse SQL Engine
```
hybridse/
├── examples          // demo db and hybrisde integration tests
├── include           // the include directory of codes, whose structure is similar to src
├── src
│   ├── base          // basic libraries catalogue
│   ├── benchmark     
│   ├── case          // cases for testing
│   ├── cmd           // packaged demo 
│   ├── codec         // decode and encode 
│   ├── codegen       // llvm codes generation
│   ├── llvm_ext      // llvm characters parsing
│   ├── node          // the definition of logic plans', physical plans' and expressions' nodes and type nodes.
│   ├── passes        // sql optimizer
│   ├── plan          // logic execution plan generation
│   ├── planv2        // the transformation from zetasql syntax tree to nodes
│   ├── proto         // the difinition of protobuf
│   ├── sdk           
│   ├── testing       
│   ├── udf           // the registration and generation of udf and udaf
│   └── vm            // the generation of sql physical plan and execution plan, the compilation and execution entries of sql 
└── tools     // codes related to benchmark 
```

## Online Storage Engine and External Service Interface 
```
src/
├── apiserver      
├── base           // basic libraries catalogue
├── catalog        
├── client         // the difinition and implementation of ns/tablet/taskmanager client interfaces 
├── cmd            // CLI and OpenMLDB binary generation
├── codec          // decode and encode 
├── log            // the formats, reading and writing of binlog and snapshot
├── nameserver     
├── proto          // definition of protobuf
├── replica        // the synchronization between leader and followers
├── rpc            // brpc request package
├── schema         // generate the resolution of schema and index 
├── sdk            
├── storage        // storage engine 
├── tablet         // the implementation of tablet interface
├── test           
├── tools          // packages of some gadgets 
└── zk             // packages of zookeeper client
```

## Java Modules
```
java/
├── hybridse-native          // codes generated automatically by SQL engine swig
├── hybridse-proto           // proto of SQL engine 
├── hybridse-sdk             // packaged sdk of SQL engine 
├── openmldb-batch           // offline planner which translates the SQL logic to the spark execution plan
├── openmldb-batchjob        // codes related to offline tasks execution 
├── openmldb-common          // some public codes and basic libraries of java sdk
├── openmldb-import          // data import tools
├── openmldb-jdbc            // java sdk
├── openmldb-jmh             // used for performance and stability testing
├── openmldb-native          // codes generated automatically by swig
├── openmldb-spark-connector // the implementation of spark connector used for reading from and writing into OpenMLDB
└── openmldb-taskmanager     // offline tasks management module 
```

## Python SDK
```
python
├── openmldb
│   ├── dbapi                // dbapi interface 
│   ├── native               // codes generated automatically by swig
│   ├── sdk                  // calling the underlying c++ interface
│   ├── sqlalchemy_openmldb  // sqlalchemy interface
│   ├── sql_magic            // notebook magic
│   └── test                 
```

## Offline Execution Engine 
https://github.com/4paradigm/spark