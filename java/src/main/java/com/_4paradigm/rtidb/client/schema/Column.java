package com._4paradigm.rtidb.client.schema;

public class Column<T>{

	private ColumnType type;
	private T value;
	public ColumnType getType() {
		return type;
	}
	public void setType(ColumnType type) {
		this.type = type;
	}
	public T getValue() {
		return value;
	}
	public void setValue(T value) {
		this.value = value;
	}	
}
