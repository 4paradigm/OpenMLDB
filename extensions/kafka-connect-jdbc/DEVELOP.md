# 开发指南

## Build

```
mvn package -DskipTests=true
```

代码格式化为Google Style，不通过会编译失败，可通过clang-format格式化。暂不支持项目内的测试，`10.5.0-SNAPSHOT-0.8.1`即使用OpenMLDB 0.8.1的发行包，可以脱离OpenMLDB项目进行打包。

## Auto Schema
kafka jdbc connector for openmldb，可以支持auto.schema，即从openmldb处获取table schema。

目前仅支持与value JsonConverter搭配使用（OpenMLDB完全不支持key），且需要disable JsonConverter的schema支持。

### 配置方法

需要修改以下的配置项：
- converter配置，可以在 connect worker `connect-xx.properties`中配置，它将改变worker上的默认converter配置，或者针对 单个connector 配置，例如修改 `openmldb-sink.properties`，也可以HTTP动态修改：
```
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```
- jdbc sink配置 `openmldb-sink.properties`：
```
auto.schema=true
auto.create=false
```
请确保已经建好了OpenMLDB表。

### message convert for auto schema

auto schema开启后，主要逻辑在[BufferedRecords](src/main/java/io/confluent/connect/jdbc/sink/BufferedRecords.java)
中。核心函数为`public List<SinkRecord> add(SinkRecord record)`。

如果没有auto schema，add中第一步便是进行validate，`recordValidator.validate(record);`。
已经设置`value.converter.schemas.enable=false`的情况下，Json Message将直接成为value段，schema==null。
于是，validate步骤会失败，抛出异常并提示"requires records with a non-null Struct value and non-null Struct schema"。

因此，支持auto schema，需要在validate之前，对Json Value进行格式转换。
Record中的value实际是需要org.apache.kafka.connect.data.Struct类型，但schemas.enable关闭的JsonConverter只会返回HashMap。
所以，此处我们将HashMap转为org.apache.kafka.connect.data.Struct，依靠从openmldb处获取的schema。

得到新的value后，重新生产新的SinkRecord，其他member保持不变。

如果想要对某个类型做更多的value转换支持，可以在`convertToLogicalType`或`convertToSchemaType`中加入逻辑。

比如，支持多类型json value转为int32类型，就在int32的case中加入对value的类型判断，并支持将该类型转为int32。

## Topic Table Mapping

支持topic到OpenMLDB表的映射配置`topic.table.mapping`，可动态更改配置，格式为`<topic>:<table>[,<topic>:<table>]`，逗号分隔。如果topic在映射中不存在，则使用原规则`table.name.format`（默认为topic名）。
