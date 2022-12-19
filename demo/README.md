# Demo Test

```
docker-compose -f docker-compose.test.yml -- up --exit-code-from sut
```
To rebuild image: 
```
docker-compose -f docker-compose.test.yml -- build
```

## Build image for snapshot

If you want to run demos on the unreleased version of OpenMLDB server or sdk, you can set flag `` and prepare the three pkgs `zookeeper.tar.gz`, `openmldb.tar.gz`, `spark-3.2.1-bin-openmldbspark.tgz` in `/demo/`(the current dir). And if s
