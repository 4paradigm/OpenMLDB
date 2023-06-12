# Python SDK/Tool Development Guideline

There are two modules in `python/`: Python SDK and an OpenMLDB diagnostic tool.

## SDK Testing Methods

Run the command `make SQL_PYSDK_ENABLE=ON OPENMLDB_BUILD_TARGET=cp_python_sdk_so` under the root directory and make sure the library in `python/openmldb_sdk/openmldb/native/` was the latest native library.
1. Package installation test: Install the compiled `whl`, then run `pytest tests/`. You can use the script `steps/test_python.sh` directly.
2. Dynamic test: Make sure there isn't OpenMLDB in `pip` or the compiled `whl`. Run `pytest test/` in `python/openmldb_sdk`, thereby you can easily debug.

You can use the following commands for partial testing
```
cd python/openmldb_sdk
pytest tests/ -k '<keyword>'
pytest tests/xxx.py::<test_func>
pytest tests/xxx.py::<test_class>::<test_func>
```
See more about `-k` in [keyword expressions](https://docs.pytest.org/en/latest/example/markers.html#using-k-expr-to-select-tests-based-on-their-name).

## Tool Testing

Since the diagnosis tools need a password-free ssh, you need to register the local user's ssh public key into the `authorized_keys` even in local testing (in which the local ssh is connected to local host).

For testing, please use:
```
cd python/openmldb_tool
pytest tests/
```

If the python log messages are required in all tests(even successful tests), please use:
```
pytest -so log_cli=true --log-cli-level=DEBUG tests/
```

## Conda

If you use conda, `pytest` may found the wrong python, then get errors like `ModuleNotFoundError: No module named 'IPython'`. Please use `python -m pytest`.
