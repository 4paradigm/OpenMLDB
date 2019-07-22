package com._4paradigm.rtidb.client.ut;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

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
	public void testCodecWithTimestamp() {
		List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
		ColumnDesc col1 = new ColumnDesc();
		col1.setAddTsIndex(true);
		col1.setName("ts");
		col1.setType(ColumnType.kTimestamp);
		schema.add(col1);
		long time = 1530772193000l;
        System.out.println(new Timestamp(time));
		try {
			ByteBuffer buffer = RowCodec.encode(new Object[] {new DateTime(time)}, schema);
//            ByteBuffer buffer = RowCodec.encode(new Object[] {time}, schema);
            buffer.rewind();
			Object[] row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(1, row.length);
			Assert.assertTrue(row[0] instanceof DateTime);
			Assert.assertEquals(time, ((DateTime)row[0]).getMillis());
			buffer = RowCodec.encode(new Object[]{null}, schema);
			buffer.rewind();
			row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(1, row.length);
			Assert.assertTrue(row[0] == null);
		} catch (TabletException e) {
			Assert.assertTrue(false);
		}
	}

	@Test
	public void testCodecWithShort() {
		List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
		ColumnDesc col1 = new ColumnDesc();
		col1.setAddTsIndex(true);
		col1.setName("ts");
		col1.setType(ColumnType.kInt16);
		schema.add(col1);
		short i = 10;
		try {
			ByteBuffer buffer = RowCodec.encode(new Object[] {i}, schema);
			buffer.rewind();
			Object[] row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(1, row.length);
			Assert.assertTrue(row[0] instanceof Short);
			Assert.assertEquals(i, row[0]);
			buffer = RowCodec.encode(new Object[]{null}, schema);
			buffer.rewind();
			row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(1, row.length);
			Assert.assertTrue(row[0] == null);
		} catch (TabletException e) {
			Assert.assertTrue(false);
		}
	}
	@Test
	public void testCodecWithBool() {
		List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
		ColumnDesc col1 = new ColumnDesc();
		col1.setAddTsIndex(true);
		col1.setName("ts");
		col1.setType(ColumnType.kBool);
		schema.add(col1);
		try {
			ByteBuffer buffer = RowCodec.encode(new Object[] {true}, schema);
			buffer.rewind();
			Object[] row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(1, row.length);
			Assert.assertTrue(row[0] instanceof Boolean);
			Assert.assertEquals(true, row[0]);
			buffer = RowCodec.encode(new Object[]{null}, schema);
			buffer.rewind();
			row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(1, row.length);
			Assert.assertTrue(row[0] == null);
		} catch (TabletException e) {
			Assert.assertTrue(false);
		}
	}
	@Test
	public void testCodecWithDate() {
		List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
		ColumnDesc col1 = new ColumnDesc();
		col1.setAddTsIndex(true);
		col1.setName("ts");
		col1.setType(ColumnType.kDate);
		schema.add(col1);
		long time = 1530772193000l;
		Date target = new Date(time);
		DateTime jodaTime=new DateTime(time);
		System.out.println(target);
        System.out.println(jodaTime);
		try {
			ByteBuffer buffer = RowCodec.encode(new Object[] {target}, schema);
			buffer.rewind();
			Object[] row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(1, row.length);
			Assert.assertTrue(row[0] instanceof Date);
			Assert.assertEquals(target, (Date)row[0]);
			buffer = RowCodec.encode(new Object[]{null}, schema);
			buffer.rewind();
			row = RowCodec.decode(buffer, schema);
			Assert.assertEquals(1, row.length);
			Assert.assertTrue(row[0] == null);
		} catch (TabletException e) {
			Assert.assertTrue(false);
		}
	}


	@Test
	public void testDecodeWithArray() {
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
			Object[] row = new Object[10];
			RowCodec.decode(buffer, schema, row, 7, 3);
			Assert.assertEquals("9527", row[7]);
			Assert.assertEquals("1234", row[8]);
			Assert.assertEquals(1.0, row[9]);
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
	@Test
	public void testEmptyString(){
//        System.out.println("testEmptyString:");
		List<ColumnDesc> schema=new ArrayList<ColumnDesc>();
		ColumnDesc col1= new ColumnDesc();
		col1.setAddTsIndex(true);
		col1.setName("empty");
		col1.setType(ColumnType.kString);
		schema.add(col1);

		ColumnDesc col2 = new ColumnDesc();
		col2.setAddTsIndex(true);
		col2.setType(ColumnType.kString);
		col2.setName("string");
		schema.add(col2);

		ColumnDesc col3 = new ColumnDesc();
		col3.setAddTsIndex(true);
		col3.setName("null");
		col3.setType(ColumnType.kInt32);
		schema.add(col3);

		ColumnDesc col4 = new ColumnDesc();
		col4.setAddTsIndex(true);
		col4.setName("empty" +
				"string is not string");
		col4.setType(ColumnType.kString);
		schema.add(col4);

		ColumnDesc col5 = new ColumnDesc();
		col5.setAddTsIndex(true);
		col5.setName("emptystring");
		col5.setType(ColumnType.kString);
		schema.add(col5);

		ColumnDesc col6 = new ColumnDesc();
		col6.setAddTsIndex(true);
		col6.setName("emptystring");
		col6.setType(ColumnType.kString);
		schema.add(col6);

		ColumnDesc col7 = new ColumnDesc();
		col7.setAddTsIndex(true);
		col7.setName("emptystring");
		col7.setType(ColumnType.kString);
		schema.add(col7);

		try{
			ByteBuffer buffer = RowCodec.encode(new Object[] {"","I am a string",null,null," ","",""},schema);
			buffer.rewind();
			Object[] row =RowCodec.decode(buffer,schema);
			Assert.assertEquals(7,row.length);
			Assert.assertEquals("",row[0]);
			Assert.assertEquals("I am a string",row[1]);
			Assert.assertEquals(null,row[2]);
			Assert.assertEquals(null,row[3]);
			Assert.assertEquals(" ",row[4]);
			Assert.assertEquals("",row[5]);
			Assert.assertEquals("",row[6]);
			for(int i=0;i<row.length;i++){
				System.out.println("schema.get(i).getType() = "+schema.get(i).getType()+" ;  value = "+ row[i]);
			}
		}catch(TabletException e){
			Assert.assertTrue(false);
		}


	}

}
