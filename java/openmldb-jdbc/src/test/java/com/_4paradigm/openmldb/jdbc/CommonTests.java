package com._4paradigm.openmldb.jdbc;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.sdk.Common;
import java.sql.Types;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CommonTests {

  @Test
  public void testConvertToDataType() throws Exception{
    DataType stringType = Common.sqlTypeToDataType(Types.VARCHAR);
    Assert.assertEquals(DataType.kTypeString, stringType);
  }

}
