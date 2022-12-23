# Demo Test

```
docker-compose -f docker-compose.test.yml -- up --exit-code-from sut
```
To rebuild image: 
```
docker-compose -f docker-compose.test.yml -- build
```

## Tests

1. quickstart, java_quickstart, python_quickstart
   
   NOTE: java project can't be built in docker container(no mvn), so you should built it by `cd java_quickstart;mvn package`. If no target jar, the test will be failed. But it won't fail the whole test.
2. taxi
3. talkingdata
4. oneflow sqls(no train or predict), a bit slow, ~10min.

After all tests done, the job checker will check all offline jobs state. It will print the job log if the job is failed.

## Build image for snapshot

If you want to run demos on the unreleased version of OpenMLDB server, you can set flag `SKIP_DOWNLOAD`, `mkdir additions`(in `/demo/`) and prepare the three pkgs `zookeeper.tar.gz`, `openmldb.tar.gz`, `spark-3.2.1-bin-openmldbspark.tgz` in `/demo/additions`.

If you want to test the unreleased version of OpenMLDB Python SDK, you can set `USE_ADD_WHL` to `true`, and prepare the whl in `/demo/addition`.
OpenMLDB Tool is the same. You can add multi whl files.

If apt is slow, copy the `sources.list` to `/demo/additions`.

If pip is slow, set `PYPI` to `tuna`.
