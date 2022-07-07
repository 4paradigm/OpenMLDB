package com._4paradigm.openmldb.test_common.chain.result;

import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.bean.SQLType;
import com._4paradigm.openmldb.test_common.util.ResultUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class ResultSetHandler extends AbstractResultHandler {

    @Override
    public boolean preHandle(SQLType sqlType) {
        return SQLType.isResultSet(sqlType);
    }

    @Override
    public void onHandle(Statement statement, OpenMLDBResult openMLDBResult) {
        try {
            ResultSet resultSet = statement.getResultSet();
            if (resultSet == null) {
                openMLDBResult.setOk(false);
                openMLDBResult.setMsg("executeSQL fail, result is null");
            } else if  (resultSet instanceof SQLResultSet){
                SQLResultSet rs = (SQLResultSet)resultSet;
                ResultUtil.setSchema(rs.getMetaData(),openMLDBResult);
                List<List<Object>> result = ResultUtil.toList(rs);
                openMLDBResult.setCount(result.size());
                openMLDBResult.setResult(result);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
