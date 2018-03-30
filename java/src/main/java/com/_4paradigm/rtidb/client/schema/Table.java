package com._4paradigm.rtidb.client.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {

	private Map<String, Integer> indexes = new HashMap<String, Integer>();
	private List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
	private int tid;
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
	
	public Table() {}
	
	public Map<String, Integer> getIndexes() {
		return indexes;
	}
	public List<ColumnDesc> getSchema() {
		return schema;
	}
	
	public static Table load(String path) throws Exception {
		BufferedReader  reader = null;
	    if (path.startsWith("classpath:")) {
            String parsedPath = path.replace("classpath:", "");
            InputStream is = Table.class.getResourceAsStream(parsedPath);
            InputStreamReader isr = new InputStreamReader(is, "utf-8");
            reader = new BufferedReader(isr);
        }else {
            reader = new BufferedReader(new FileReader(new File(path)));
        }
	    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
	    String line = null;
	    while ((line = reader.readLine()) != null) {
	    	ColumnDesc column = parse(line);
	    	
	    	if (column == null) {
	    		continue;
	    	}
	    	schema.add(column);
	    	
	    }
	    return new Table(schema);
	}
	
	private static ColumnDesc parse(String line) {
        if (line == null || line.equals("") == true || line.startsWith("#")) {
            return null;
        }
        ColumnDesc column = new ColumnDesc();
        for (String part : line.split(";")) {
            String[] express = part.split("=");
            if (express.length < 2) {
                continue;
            }
            if (express[0].equalsIgnoreCase("column")) {
                column.setName(express[1]);
            }else if (express[0].equalsIgnoreCase("type")) {
                if (express[1].equalsIgnoreCase("int32")) {
                    column.setType(ColumnType.kInt32);
                }else if (express[1].equalsIgnoreCase("float")) {
                    column.setType(ColumnType.kFloat);
                }else if (express[1].equalsIgnoreCase("string")) {
                    column.setType(ColumnType.kString);
                }else if (express[1].equalsIgnoreCase("int64")) {
                    column.setType(ColumnType.kInt64);
                }else if (express[1].equalsIgnoreCase("double")) {
                    column.setType(ColumnType.kDouble);
                }
            }
        }
        return column;
    }

	public int getTid() {
		return tid;
	}

	public void setTid(int tid) {
		this.tid = tid;
	}

	@Override
	public String toString() {
		return "Table [schema_size=" + schema.size() + ", tid=" + tid + "]";
	}
	
	
}
