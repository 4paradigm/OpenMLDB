# Demo Test

```
docker-compose -f docker-compose.test.yml -- up --exit-code-from sut
```
To rebuild image: 
```
docker-compose -f docker-compose.test.yml -- build
```

## Build image for snapshot

If you want to run demos on the unreleased version of OpenMLDB server or sdk, you can set flag `SKIP_DOWNLOAD`, `mkdir additions`(in `/demo/`) and prepare the three pkgs `zookeeper.tar.gz`, `openmldb.tar.gz`, `spark-3.2.1-bin-openmldbspark.tgz` in `/demo/additions`.

If apt is slow, copy the `sources.list` to `/demo/additions`.
