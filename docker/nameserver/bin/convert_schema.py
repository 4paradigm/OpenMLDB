import sys
import json

rtidb_type = {"SmallInt": "int32", "Int": "int32", "BigInt": "int64", "Float": "float", "Double": "double", "String": "string", "Date": "string", "Timestamp": "int64"}

schema_file = open(sys.argv[1], 'r')
data = schema_file.read()
json_data = json.loads(data)
print("name : \"transaction\"")
print("ttl: 0")
print("ttl_type : \"kAbsoluteTime\"")
for item in json_data:
    print("column_desc {")
    print("  name : \"" + item["name"] + "\"")
    print("  type : \"" + rtidb_type[item["type"]] + "\"")
    print("  add_ts_idx : false ")
    print("}")
