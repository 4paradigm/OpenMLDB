# 错误码

客户端执行返回的状态类是`hybridse::sdk::Status`，错误码使用StatusCode。但有些错误是服务端连接错误或返回错误`openmldb::base::Status`，我们也需要分辨它们，所以`hybridse::sdk::Status`的错误信息中可能会追加`openmldb::base::Status`的错误码与错误信息，`openmldb::base::Status`使用的错误码ReturnCode会用`ReturnCode[xxx]`标识。

例如：
```
Error: [2001] async offline query failed--ReturnCode[1003]--Fail to get TaskManager client
```
这个错误中2001为`StatusCode::kServerError`，它提示真实错误是`openmldb::base::Status`，需要查ReturnCode和其后的错误信息。`--ReturnCode`前的信息可能提示调用栈，方便追踪问题的链路。而`ReturnCode[1003]`是`ReturnCode::kServerConnError`，它说明客户端连接服务端出错，请检查服务端是否存活，或网络连接是否稳定。

## StatusCode 错误码
StatusCode 定义在[fe_common.proto](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/proto/fe_common.proto)。

常见错误的打印内容列表如下：

| 错误码       | 数值 | 说明                                                                                                                                                                            | 日志 |
| ------------ | ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- |
| kRpcError    | 1500 | 与kServerError不同，强调RPC错误，出现在客户端直连Tablet服务端执行Query/CallProcedure等RPC的时候。可能是RPC本身出错（与ReturnCode::kRPCError类似），也可能是RPC Response返回错误 |      |
| kConnError   | 1501 | 与ReturnCode::kServerConnError等价，只是直接返回`hybridse::sdk::Status`和接收`openmldb::base::Status`再返回的区别                                                               |      |
| kCmdError    | 2000 | CMD SQL执行错误，通常是客户端解析或执行DELETE时出错                                                                                                                             |      |
| kServerError | 2001 | 请查[ReturnCode 错误码](#returncode-错误码)。（其中也可能包含较底层的RPC错误）                                                                                                  |      |

## ReturnCode 错误码

ReturnCode 定义在[base/status.h](https://github.com/4paradigm/OpenMLDB/blob/main/src/base/status.h)，通常出现在连接服务端失败、服务端执行产生错误的时候。

常见错误的打印内容列表如下：

| 错误码           | 数值 | 说明                                                                                                                                                                                                                      | 日志                             |
| ---------------- | ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| kError           | -1   | 没有单独错误码的错误，重点看错误信息                                                                                                                                                                                      |
| kSQLCmdRunError  | 901  | 客户端执行SQL失败，可能在本地导入导出、创建表或存储过程时出现                                                                                                                                                             |                                  |
| kSQLCompileError | 1000 | Tablet服务端中SQL编译错误，极少出现                                                                                                                                                                                       |                                  |
| kSQLRunError     | 1001 | Tablet服务端执行查询时出错                                                                                                                                                                                                |                                  |
| kRPCRunError     | 1002 | 服务端执行RPC出错，目前只有create/drop function会报这个错误                                                                                                                                                               |                                  |
| kServerConnError | 1003 | 客户端连接服务端出错，通常出现在连接taskmanager失败，可能taskmanager并未启动或在集群中                                                                                                                                    | "Fail to get TaskManager client" |
| kRPCError        | 1004 | 客户端发送RPC失败，错误信息为brpc的错误码与错误信息。通常是连接中断或RPC超时等，具体可以查询[brpc错误码](https://github.com/4paradigm/incubator-brpc/blob/a85d1bde8df3a3e2e59a64ea5a3ee3122f9c6daa/docs/cn/error_code.md) |                                  |
