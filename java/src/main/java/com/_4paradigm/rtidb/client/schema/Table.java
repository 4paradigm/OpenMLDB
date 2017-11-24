package com._4paradigm.rtidb.client.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {

	private Map<String, Integer> indexes = new HashMap<String, Integer>();
	private List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
	public Table(List<ColumnDesc> schema) {
		this.schema = schema;
		int index = 0;
		for (ColumnDesc col : schema) {
			if (col.isAddTsIndex()) {
				indexes.put(col.getName(), index);
				index ++;
			}
		}
	}
	public Map<String, Integer> getIndexes() {
		return indexes;
	}
	public List<ColumnDesc> getSchema() {
		return schema;
	}
	
	
}
