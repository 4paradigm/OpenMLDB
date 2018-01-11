package com._4paradigm.rtidb.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.SchemaCodec;

public class SchemaCodecTest {

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
		
		ByteBuffer buffer;
		try {
			buffer = SchemaCodec.encode(schema);
			buffer.rewind();
			List<ColumnDesc> nschema = SchemaCodec.decode(buffer);
			
			Assert.assertEquals(schema.size(), nschema.size());
			
			
			Assert.assertTrue(nschema.get(0).isAddTsIndex());
			Assert.assertEquals("card", nschema.get(0).getName());
			Assert.assertEquals(ColumnType.kString, nschema.get(0).getType());
			
			Assert.assertTrue(nschema.get(1).isAddTsIndex());
			Assert.assertEquals("merchant", nschema.get(1).getName());
			Assert.assertEquals(ColumnType.kString, nschema.get(1).getType());
			

			Assert.assertFalse(nschema.get(2).isAddTsIndex());
			Assert.assertEquals("amt", nschema.get(2).getName());
			Assert.assertEquals(ColumnType.kDouble, nschema.get(2).getType());
		} catch (TabletException e) {
			Assert.assertTrue(false);
		}
		
		
	}
}
