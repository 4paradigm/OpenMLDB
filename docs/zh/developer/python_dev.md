# Python SDK/Tool 开发指南

`python/`中有两个组件，一个Python SDK，一个诊断工具OpenMLDB Tool。

## SDK

Python SDK本身不依赖测试用的pytest和tox库，如果你想使用tests测试文件进行测试，可以通过以下方式下载测试依赖。

```
pip install 'openmldb[test]'
pip install 'dist/....whl[test]'
```

### 测试方法

在根目录执行`make SQL_PYSDK_ENABLE=ON OPENMLDB_BUILD_TARGET=cp_python_sdk_so`，确保`python/openmldb_sdk/openmldb/native/`中使用的是最新的native库。测试通常需要连接一个OpenMLDB集群，如果你还没有启动集群，或对服务组件有代码改动，还需要编译TARGET openmldb，并启动onebox集群，可参考`steps/test_python.sh`的启动部分。

1. 安装包测试：安装编译好的whl，再`pytest test/`。可直接使用脚本`steps/test_python.sh`。
1. 动态测试：确认pip中无openmldb，也不要安装编译好的whl，在`python/openmldb_sdk`中执行`pytest tests/`即可。这种方式可以方便调试代码。

只运行部分测试，可以使用：
```
cd python/openmldb_sdk
pytest tests/ -k '<keyword>'
pytest tests/xxx.py::<test_func>
pytest tests/xxx.py::<test_class>::<test_func>
```
`-k`使用方式见[keyword expressions](https://docs.pytest.org/en/latest/example/markers.html#using-k-expr-to-select-tests-based-on-their-name)。

## Tool 测试

由于Tool中的诊断工具静态诊断需要ssh免密（其他子命令不需要ssh），所以，即使在本地测试（本地ssh到本地），也需要将当前用户的ssh pub key写入当前用户的authorized_keys。

普通测试：
```
cd python/openmldb_tool
pytest tests/
```

测试如果需要python log信息（成功的测试也会打印）:
```
pytest -so log_cli=true --log-cli-level=DEBUG tests/
```

## Conda

如果使用Conda环境，`pytest`命令可能找到错误的python环境，而导致类似`ModuleNotFoundError: No module named 'IPython'`的问题。请使用`python -m pytest`。
