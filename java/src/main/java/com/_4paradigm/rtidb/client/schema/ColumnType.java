package com._4paradigm.rtidb.client.schema;

public enum ColumnType {
	kString(0), kFloat(1), kInt32(2), kInt64(3), kDouble(4), kNull(5), kUInt32(6), kUInt64(7), kTimestamp(8), kDate(9), kInt16(10), kBool(12);

	private final int value;

	private ColumnType(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}
	
	public String toString() {
		switch(value) {
		case 0:
			return "kString";
		case 1:
			return "kFloat";
		case 2:
			return "kInt32";
		case 3:
			return "kInt64";
		case 4:
			return "kDouble";
		case 5:
			return "kNull";
		case 6:
			return "kUInt32";
		case 7:
		    return "kUInt64";
		case 8:
		    return "kTimestamp";
		case 9:
		    return "kDate";
		case 10:
		    return "kInt16";
		case 12:
		    return "kBool";
		default :
			return "Unknow";
		}
	}

	public static ColumnType valueOf(int val) {
		switch(val) {
		case 0:
			return kString;
		case 1:
			return kFloat;
		case 2:
			return kInt32;
		case 3:
			return kInt64;
		case 4:
			return kDouble;
		case 5:
			return kNull;
		case 6:
			return kUInt32;
		case 7:
		    return kUInt64;
		case 8:
		    return kTimestamp;
		case 9:
		    return kDate;
		case 10:
		    return kInt16;
		case 12:
		    return kBool;
		default :
			return null;
		}
	}
	
	public static ColumnType valueFrom(String val) {
	    if ("string".equals(val)) {
	        return kString;
	    }else if ("int32".equals(val)) {
	        return kInt32;
	    }else if ("double".equals(val)) {
	        return kDouble;
	    }else if ("int64".equals(val)) {
	        return kInt64;
	    }else if ("float".equals(val)) {
	        return kFloat;
	    }else if ("uint32".equals(val)) {
	        return kUInt32;
	    }else if ("uint64".equals(val)) {
	        return kUInt64;
	    }else if ("timestamp".equals(val)) {
	        return kTimestamp;
	    }else if ("date".equals(val)) {
	        return kDate;
	    }else if ("int16".equals(val)) {
	       return kInt16; 
	    }else if ("bool".equals(val)) {
	        return kBool;
	    }
	    return null;
	}
	
}
