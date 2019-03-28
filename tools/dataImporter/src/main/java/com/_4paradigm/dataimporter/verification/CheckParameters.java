package com._4paradigm.dataimporter.verification;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CheckParameters {
    private static Logger logger = LoggerFactory.getLogger(CheckParameters.class);

    public static boolean checkPutParameters(String path, String tableName, List<ColumnDesc> schemaList) {
        if (path == null || path.trim().equals("")) {
            logger.error("the path of the file does not exist！");
            return true;
        }
        if (tableName == null || tableName.trim().equals("")) {
            logger.error("the table does not exist！");
            return true;
        }
        if (schemaList == null) {
            logger.error("schemaList is null!");
            return true;
        }
        return false;
    }
}
