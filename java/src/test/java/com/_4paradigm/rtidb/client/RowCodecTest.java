package com._4paradigm.rtidb.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

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
}
