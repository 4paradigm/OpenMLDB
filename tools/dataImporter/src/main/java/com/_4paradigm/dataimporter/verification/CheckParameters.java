package com._4paradigm.dataimporter.verification;

import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckParameters {
    private static Logger logger = LoggerFactory.getLogger(CheckParameters.class);

    public static boolean checkPutParameters(String path, String tableName, MessageType schema) {
        if (path == null || path.trim().equals("")) {
            logger.error("the path of the file does not exist！");
            return true;
        }
        if (tableName == null || tableName.trim().equals("")) {
            logger.error("the table does not exist！");
            return true;
        }
        if (schema == null) {
            logger.error("schema is null!");
            return true;
        }
        return false;
    }
}
