package com._4paradigm.rtidb.client.ut;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.RowCodec;

public class RowCodecTest {

	@Test
	public void testCodec() {
		List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
		ColumnDesc col1 = new ColumnDesc();
		col1.setAddTsIndex(true);
		col1.setName("card");
		col1.setType(ColumnType.kString);
		schema.add(col1);
		
		ColumnDesc col2 = new ColumnDesc();
		col2.setAddTsIndex(true);
		col2.setName("merchant");
		col2.setType(ColumnType.kString);
		schema.add(col2);
		
		ColumnDesc col3 = new ColumnDesc();
		col3.setAddTsIndex(false);
		col3.setName("amt");
		col3.setType(ColumnType.kDouble);
		schema.add(col3);
		
		try {
			ByteBuffer buffer = RowCodec.encode(new Object[] {"9527", "1234", 1.0}, schema);
			buffer.rewind();
			Object[] row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(3, row.length);
			Assert.assertEquals("9527", row[0]);
			Assert.assertEquals("1234", row[1]);
			Assert.assertEquals(1.0, row[2]);
		} catch (TabletException e) {
			Assert.assertTrue(false);
		}
	}
	
	@Test
	public void testCodecPerf() {
		List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
		ColumnDesc col1 = new ColumnDesc();
		col1.setAddTsIndex(true);
		col1.setName("card");
		col1.setType(ColumnType.kString);
		schema.add(col1);
		
		ColumnDesc col2 = new ColumnDesc();
		col2.setAddTsIndex(true);
		col2.setName("merchant");
		col2.setType(ColumnType.kDouble);
		schema.add(col2);
		
		ColumnDesc col3 = new ColumnDesc();
		col3.setAddTsIndex(false);
		col3.setName("amt");
		col3.setType(ColumnType.kInt64);
		schema.add(col3);
		
		ColumnDesc col4 = new ColumnDesc();
		col4.setAddTsIndex(false);
		col4.setName("amt1");
		col4.setType(ColumnType.kInt32);
		schema.add(col4);
		
		ColumnDesc col5 = new ColumnDesc();
		col5.setAddTsIndex(false);
		col5.setName("amt3");
		col5.setType(ColumnType.kFloat);
		schema.add(col5);
		
		
		try {
			ByteBuffer buffer = RowCodec.encode(new Object[] {"12345678910", 1.1d, 1000l, 1000, 2.1f}, schema);
			int i = 1000;
			while (i > 0) {
				buffer.rewind();
				Object[] row = RowCodec.decode(buffer, schema);
				i--;
			}
		    i = 100000;
		    Long consumed = System.nanoTime();
			while (i > 0) {
				buffer.rewind();
				Object[] row = RowCodec.decode(buffer, schema);
				i--;
			}
			consumed = System.nanoTime() - consumed;
			System.out.println(consumed);
		} catch (TabletException e) {
			Assert.assertTrue(false);
		}
	}
	
}
