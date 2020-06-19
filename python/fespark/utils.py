
import json

"""
Utils from pyspark.
"""
def _parse_datatype_json_string(json_string):
    return _parse_datatype_json_value(json.loads(json_string))

def _parse_datatype_json_value(json_value):
    from pyspark.sql.types import ArrayType
    from pyspark.sql.types import MapType
    from pyspark.sql.types import StructType

    _all_complex_types = dict((v.typeName(), v)
                                for v in [ArrayType, MapType, StructType])

    if not isinstance(json_value, dict):
        if json_value in _all_atomic_types.keys():
            return _all_atomic_types[json_value]()
        elif json_value == 'decimal':
            return DecimalType()
        elif _FIXED_DECIMAL.match(json_value):
            m = _FIXED_DECIMAL.match(json_value)
            return DecimalType(int(m.group(1)), int(m.group(2)))
        else:
            raise ValueError("Could not parse datatype: %s" % json_value)
    else:
        tpe = json_value["type"]
        if tpe in _all_complex_types:
            return _all_complex_types[tpe].fromJson(json_value)
        elif tpe == 'udt':
            return UserDefinedType.fromJson(json_value)
        else:
            raise ValueError("not supported type: %s" % tpe)

def _to_corrected_pandas_type(dt):
    from pyspark.sql.types import ByteType
    from pyspark.sql.types import ShortType
    from pyspark.sql.types import IntegerType
    from pyspark.sql.types import FloatType
    import numpy as np

    if type(dt) == ByteType:
        return np.int8
    elif type(dt) == ShortType:
        return np.int16
    elif type(dt) == IntegerType:
        return np.int32
    elif type(dt) == FloatType:
        return np.float32
    else:
        return None
