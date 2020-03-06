package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.type.DataType;

public class ColumnDesc {

	private ColumnType type;
	private String name;
	private boolean addTsIndex;
	private boolean tsCol;
	private DataType dataType;
	private boolean notNull;

	public ColumnDesc() {
	}

	public boolean isAddTsIndex() {
		return addTsIndex;
	}
	public void setAddTsIndex(boolean addTsIndex) {
		this.addTsIndex = addTsIndex;
	}
	public ColumnType getType() {
		return type;
	}
	public void setType(ColumnType type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public boolean isTsCol() {
		return tsCol;
	}
	public void setTsCol(boolean isTsCol) {
		this.tsCol = isTsCol;
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	public boolean isNotNull() {
		return notNull;
	}

	public void setNotNull(boolean notNull) {
		this.notNull = notNull;
	}
}
