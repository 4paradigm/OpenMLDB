# Error Code

The status class returned by client execution is `hybridse::sdk::Status`, and error codes are represented using `StatusCode`. However, certain errors may be related to server connection issues or return errors like `openmldb::base::Status`. To distinguish between these types of errors, the error message for `hybridse::sdk::Status` may include an error code and information related to `openmldb::base::Status`. The error code utilized for `openmldb::base::Status`, known as `ReturnCode`, is identified by `ReturnCode[xxx]`.

For example:

```
Error: [2001] async offline query failed--ReturnCode[1003]--Fail to get TaskManager client
```

In this error message, the code 2001 corresponds to `StatusCode::kServerError`, indicating that the actual error is within `openmldb::base::Status`. To pinpoint the issue, you should examine the `ReturnCode` and the subsequent error details. The information preceding `--ReturnCode` might provide a call stack, which can be helpful in tracing the problem's source. On the other hand, `ReturnCode[1003]` represents `ReturnCode::kServerConnError`, indicating a connection error on the client side when connecting to the server. To resolve this, please ensure that the server is operational and that the network connection is stable.

## StatusCode Error Code

StatusCode is defined in [fe_common.proto](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/proto/fe_common.proto).

The list of common printing errors is as follows:

| Error Code   | Numerical Value | Description                                                  |
| ------------ | --------------- | ------------------------------------------------------------ |
| kRpcError    | 1500            | Unlike `kServerError`, `kRPCError` is specifically focused on RPC errors. These errors occur when the client establishes a direct connection to the tablet server for executing RPC operations such as Query or CallProcedure. They can be either RPC-related errors themselves (similar to `ReturnCode::kRPCError`) or errors returned within the RPC Response. |
| kConnError   | 1501            | This error code is similar to `ReturnCode::kServerConnError`, with the primary distinction being in how errors are directly returned as `hybridse::sdk::Status` instead of receiving an `openmldb::base::Status` before being returned. |
| kCmdError    | 2000            | Another type of error pertains to command-line SQL execution errors, which are typically caused by issues with client parsing or executing DELETE commands. |
| kServerError | 2001            | For a comprehensive list of error codes, including lower-level RPC errors, please refer to the [ReturnCode Error Code](https://chat.openai.com/c/03700398-f563-4565-8fae-132a05b01efb#returncode-errorcode). |

## ReturnCode Error Code

ReturnCode is defined in [base/status.h](https://github.com/4paradigm/OpenMLDB/blob/main/src/base/status.h). It usually occurs when the connection to the server fails or an error occurs during server execution.

The list of common printing errors is as follows:

| Error Code       | Numerical Value | Description                                                  |
| ---------------- | --------------- | ------------------------------------------------------------ |
| kError           | -1              | Errors that lack distinct error codes are typically identified by their error messages. |
| kSQLCmdRunError  | 901             | **Client SQL Execution Failure**: This can happen during various client-side operations, such as local imports and exports, table creation, or stored procedure creation. |
| kSQLCompileError | 1000            | **SQL Compilation Errors**: These errors are rare in the Tablet server and typically occur during SQL compilation. |
| kSQLRunError     | 1001            | **Query Execution Error on Tablet Server**: An error occurred while executing a query on the Tablet server. |
| kRPCRunError     | 1002            | **RPC Execution Error on Server**: Currently, only the CREATE and DROP JUNCTION commands report this error when executing an RPC on the server. |
| kServerConnError | 1003            | **Client-Server Connection Error**: This error happens when there's an issue connecting the client to the server. It's often caused by a failure to connect to the TaskManager. This could occur if the TaskManager has not been started or is in a cluster. |
| kRPCError        | 1004            | **RPC Sending Failure**: When the client fails to send an RPC, the error message includes the error code and message from brpc. Typically, this happens due to a connection interruption or RPC timeout. You can refer to the [brpc Error Code](https://github.com/4paradigm/incubator-brpc/blob/a85d1bde8df3a3e2e59a64ea5a3ee3122f9c6daa/docs/cn/error_code.md) for more details on the error codes. |