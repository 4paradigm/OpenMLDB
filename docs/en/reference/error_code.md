# Error Codes

The status returned by the client execution is of type `hybridse::sdk::Status`, and error codes are represented using `StatusCode`. However, some errors are related to server connection or are returned as `openmldb::base::Status`. To distinguish between them, the error information in `hybridse::sdk::Status` may include the error code and error message from `openmldb::base::Status`. The error codes used by `openmldb::base::Status` are indicated by `ReturnCode[xxx]`.

For example:
```
Error: [2001] async offline query failed--ReturnCode[1003]--Fail to get TaskManager client
```
In this error, 2001 is `StatusCode::kServerError`, indicating that the actual error is in `openmldb::base::Status`. You need to check the `ReturnCode` and the subsequent error message. The information before `--ReturnCode` might provide a call stack, facilitating the tracing of the problem chain. `ReturnCode[1003]` is `ReturnCode::kServerConnError`, indicating a client-side connection error to the server. Please check if the server is alive and the network connection is stable.

## StatusCode Error Codes
`StatusCode` is defined in [fe_common.proto](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/proto/fe_common.proto).

Here is a list of commonly printed error codes:

| Error Code | Value | Description |
| ---------- | ----- | ----------- |
| kRpcError  | 1500  | Emphasizes RPC errors, occurs when executing Query/CallProcedure RPC directly connecting to the tablet server. It may indicate an RPC error or an error in the RPC response. |
| kConnError | 1501  | Equivalent to `ReturnCode::kServerConnError`, but returned directly as `hybridse::sdk::Status` without receiving `openmldb::base::Status`. |
| kCmdError  | 2000  | Command-line SQL execution error, usually occurs when the client parsing or executing DELETE encounters an error. |
| kServerError | 2001 | Please refer to [ReturnCode Error Codes](#returncode-error-codes). (It may also include lower-level RPC errors.)

## ReturnCode Error Codes
`ReturnCode` is defined in [base/status.h](https://github.com/4paradigm/OpenMLDB/blob/main/src/base/status.h) and is commonly used for connection failures and errors produced during server execution.

Here is a list of commonly printed error codes:

| Error Code      | Value | Description |
| --------------- | ----- | ----------- |
| kError          | -1    | No specific error code, focus on the error message. |
| kSQLCmdRunError | 901   | Client execution of SQL failed, may occur during local import/export, table or stored procedure creation. |
| kSQLCompileError| 1000  | SQL compilation error in the Tablet server, very rare. |
| kSQLRunError    | 1001  | Error occurred when the Tablet server executed a query. |
| kRPCRunError    | 1002  | RPC execution error on the server side, currently only reported for CREATE/DROP FUNCTION commands. |
| kServerConnError| 1003  | Client failed to connect to the server, usually occurs when connecting to the TaskManager, indicating that the TaskManager may not be started or is not reachable in the cluster. |
| kRPCError       | 1004  | RPC failure on the client side, with the error code and message from brpc. This is typically caused by connection interruption or RPC timeout, and specific details can be found in the [brpc error codes](https://github.com/4paradigm/incubator-brpc/blob/a85d1bde8df3a3e2e59a64ea5a3ee3122f9c6daa/docs/cn/error_code.md). |