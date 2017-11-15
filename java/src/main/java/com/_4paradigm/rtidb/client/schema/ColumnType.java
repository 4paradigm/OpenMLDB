package com._4paradigm.rtidb.client.schema;

public enum ColumnType {
	kString(0), kFloat(1), kInt32(2), kInt64(3), kDouble(4), kNull(5), kUInt32(6), kUInt64(7);

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
		default :
			return kUInt64;
		}
	}
}
