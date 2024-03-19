package com._4paradigm.openmldb.mysql.mock;

import cn.paxos.mysql.MySqlListener;
import cn.paxos.mysql.engine.QueryResultColumn;
import com._4paradigm.openmldb.common.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockResult {
  public static Map<String, Pair<List<QueryResultColumn>, List<List<String>>>> mockResults =
      new HashMap<>();
  public static Map<String, Pair<List<QueryResultColumn>, List<List<String>>>> mockPatternResults =
      new HashMap<>();

  public static Map<String, String> mockVariables = new HashMap<>();

  public static Map<String, String> mockSessionVariables = new HashMap<>();

  static {
    String query;
    List<QueryResultColumn> columns;
    List<List<String>> rows;
    List<String> row;

    mockVariables.put("character_set_database", "utf8mb4");
    mockVariables.put("collation_database", "utf8mb4_0900_ai_ci");
    mockVariables.put("default_storage_engine", "InnoDB");
    mockVariables.put("skip_show_database", "OFF");
    mockVariables.put("version", MySqlListener.VERSION);
    mockVariables.put("version_comment", MySqlListener.VERSION_COMMENT);
    for (String variable : MockResult.mockVariables.keySet()) {
      query = "show variables like '" + variable.toLowerCase() + "'";
      columns = new ArrayList<>();
      columns.add(new QueryResultColumn("Variable_name", "VARCHAR(255)"));
      columns.add(new QueryResultColumn("Value", "VARCHAR(255)"));
      rows = new ArrayList<>();
      row = new ArrayList<>();
      row.add(variable);
      row.add(mockVariables.get(variable));
      rows.add(row);
      mockResults.put(query, new Pair<>(columns, rows));
    }

    query = "show character set";
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Charset", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Description", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Default collation", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Maxlen", "VARCHAR(255)"));
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("utf8mb4");
    row.add("UTF-8 Unicode");
    row.add("utf8mb4_0900_ai_ci");
    row.add("4");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    query = "show character set where charset = 'utf8mb4'";
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Charset", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Description", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Default collation", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Maxlen", "VARCHAR(255)"));
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("utf8mb4");
    row.add("UTF-8 Unicode");
    row.add("utf8mb4_0900_ai_ci");
    row.add("4");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    query = "show collation";
    // Collation	Charset	Id	Default	Compiled	Sortlen	Pad_attribute
    // utf8mb4_0900_ai_ci	utf8mb4	255	Yes	Yes	0	NO PAD
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Collation", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Charset", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Id", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Default", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Compiled", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Sortlen", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Pad_attribute", "VARCHAR(255)"));
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("utf8mb4_0900_ai_ci");
    row.add("utf8mb4");
    row.add("255");
    row.add("Yes");
    row.add("Yes");
    row.add("0");
    row.add("NO PAD");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    query = "show engines";
    // Engine	Support	Comment	Transactions	XA	Savepoints
    // InnoDB	DEFAULT	Supports transactions, row-level locking, and foreign keys	YES	YES	YES
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Engine", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Support", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Comment", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Transactions", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("XA", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Savepoints", "VARCHAR(255)"));
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("InnoDB");
    row.add("DEFAULT");
    row.add("Supports transactions, row-level locking, and foreign keys");
    row.add("YES");
    row.add("YES");
    row.add("YES");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    query = "show global status";
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Variable_name", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Value", "VARCHAR(255)"));
    rows = new ArrayList<>();
    mockResults.put(query, new Pair<>(columns, rows));

    query =
        "show variables like 'lower_case_%'; show variables like 'sql_mode'; select count(*) as support_ndb from information_schema.engines where engine = 'ndbcluster'";
    columns = new ArrayList<>();
    // # support_ndb
    // 0
    columns.add(new QueryResultColumn("support_ndb", "VARCHAR(255)"));
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("0");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    mockSessionVariables.put(
        "sql_mode",
        "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION");
    mockSessionVariables.put("Ssl_cipher", "TLS_AES_256_GCM_SHA384");
    mockSessionVariables.put("version_comment", MySqlListener.VERSION_COMMENT);
    mockSessionVariables.put("version", MySqlListener.VERSION);
    for (String sessionVariable : MockResult.mockSessionVariables.keySet()) {
      query = "show session status like '" + sessionVariable.toLowerCase() + "'";
      columns = new ArrayList<>();
      columns.add(new QueryResultColumn("Variable_name", "VARCHAR(255)"));
      columns.add(new QueryResultColumn("Value", "VARCHAR(255)"));
      rows = new ArrayList<>();
      row = new ArrayList<>();
      row.add(sessionVariable);
      row.add(mockSessionVariables.get(sessionVariable));
      rows.add(row);
      mockResults.put(query, new Pair<>(columns, rows));
    }

    query = "show plugins";
    // # Name, Status, Type, Library, License
    // keyring_file, ACTIVE, KEYRING, keyring_file.so, GPL
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Name", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Status", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Type", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Library", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("License", "VARCHAR(255)"));
    rows = new ArrayList<>();
    // # Name, Status, Type, Library, License
    // mysql_native_password, ACTIVE, AUTHENTICATION, , GPL
    row = new ArrayList<>();
    row.add("mysql_native_password");
    row.add("ACTIVE");
    row.add("AUTHENTICATION");
    row.add("");
    row.add("GPL");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    query = "show slave status";
    // Slave_IO_State, Master_Host, Master_User, Master_Port, Connect_Retry, Master_Log_File,
    // Read_Master_Log_Pos, Relay_Log_File, Relay_Log_Pos, Relay_Master_Log_File, Slave_IO_Running,
    // Slave_SQL_Running, Replicate_Do_DB, Replicate_Ignore_DB, Replicate_Do_Table,
    // Replicate_Ignore_Table, Replicate_Wild_Do_Table, Replicate_Wild_Ignore_Table, Last_Errno,
    // Last_Error, Skip_Counter, Exec_Master_Log_Pos, Relay_Log_Space, Until_Condition,
    // Until_Log_File, Until_Log_Pos, Master_SSL_Allowed, Master_SSL_CA_File, Master_SSL_CA_Path,
    // Master_SSL_Cert, Master_SSL_Cipher, Master_SSL_Key, Seconds_Behind_Master,
    // Master_SSL_Verify_Server_Cert, Last_IO_Errno, Last_IO_Error, Last_SQL_Errno, Last_SQL_Error,
    // Replicate_Ignore_Server_Ids, Master_Server_Id, Master_UUID, Master_Info_File, SQL_Delay,
    // SQL_Remaining_Delay, Slave_SQL_Running_State, Master_Retry_Count, Master_Bind,
    // Last_IO_Error_Timestamp, Last_SQL_Error_Timestamp, Master_SSL_Crl, Master_SSL_Crlpath,
    // Retrieved_Gtid_Set, Executed_Gtid_Set, Auto_Position, Replicate_Rewrite_DB, Channel_Name,
    // Master_TLS_Version, Master_public_key_path, Get_master_public_key, Network_Namespace
    columns = new ArrayList<>();
    String columnNameStr =
        "Slave_IO_State, Master_Host, Master_User, Master_Port, Connect_Retry, Master_Log_File, Read_Master_Log_Pos, Relay_Log_File, Relay_Log_Pos, Relay_Master_Log_File, Slave_IO_Running, Slave_SQL_Running, Replicate_Do_DB, Replicate_Ignore_DB, Replicate_Do_Table, Replicate_Ignore_Table, Replicate_Wild_Do_Table, Replicate_Wild_Ignore_Table, Last_Errno, Last_Error, Skip_Counter, Exec_Master_Log_Pos, Relay_Log_Space, Until_Condition, Until_Log_File, Until_Log_Pos, Master_SSL_Allowed, Master_SSL_CA_File, Master_SSL_CA_Path, Master_SSL_Cert, Master_SSL_Cipher, Master_SSL_Key, Seconds_Behind_Master, Master_SSL_Verify_Server_Cert, Last_IO_Errno, Last_IO_Error, Last_SQL_Errno, Last_SQL_Error, Replicate_Ignore_Server_Ids, Master_Server_Id, Master_UUID, Master_Info_File, SQL_Delay, SQL_Remaining_Delay, Slave_SQL_Running_State, Master_Retry_Count, Master_Bind, Last_IO_Error_Timestamp, Last_SQL_Error_Timestamp, Master_SSL_Crl, Master_SSL_Crlpath, Retrieved_Gtid_Set, Executed_Gtid_Set, Auto_Position, Replicate_Rewrite_DB, Channel_Name, Master_TLS_Version, Master_public_key_path, Get_master_public_key, Network_Namespace";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    mockResults.put(query, new Pair<>(columns, rows));

    query = "show tables in information_schema like 'engines'";
    // # Tables_in_information_schema (ENGINES)
    // ENGINES
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Tables_in_information_schema (ENGINES)", "VARCHAR(255)"));
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("ENGINES");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    query =
        "select engine, support from `information_schema`.`engines` where support in ('default', 'yes') and engine != 'performance_schema'";
    // # Engine, Support
    // ARCHIVE, YES
    // BLACKHOLE, YES
    // MRG_MYISAM, YES
    // MyISAM, YES
    // InnoDB, DEFAULT
    // MEMORY, YES
    // CSV, YES
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Engine", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Support", "VARCHAR(255)"));
    rows = new ArrayList<>();
    //    row = new ArrayList<>();
    //    row.add("ARCHIVE");
    //    row.add("YES");
    //    rows.add(row);
    //    row = new ArrayList<>();
    //    row.add("BLACKHOLE");
    //    row.add("YES");
    //    rows.add(row);
    //    row = new ArrayList<>();
    //    row.add("MRG_MYISAM");
    //    row.add("YES");
    //    rows.add(row);
    //    row = new ArrayList<>();
    //    row.add("MyISAM");
    //    row.add("YES");
    //    rows.add(row);
    row = new ArrayList<>();
    row.add("InnoDB");
    row.add("DEFAULT");
    rows.add(row);
    //    row = new ArrayList<>();
    //    row.add("MEMORY");
    //    row.add("YES");
    //    rows.add(row);
    //    row = new ArrayList<>();
    //    row.add("CSV");
    //    row.add("YES");
    //    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    query = "select * from `information_schema`.`character_sets` order by `character_set_name` asc";
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("CHARACTER_SET_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("DEFAULT_COLLATE_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("DESCRIPTION", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("MAXLEN", "VARCHAR(255)"));
    rows = new ArrayList<>();
    // # CHARACTER_SET_NAME, DEFAULT_COLLATE_NAME, DESCRIPTION, MAXLEN
    // latin1, latin1_swedish_ci, cp1252 West European, 1
    row = new ArrayList<>();
    row.add("latin1");
    row.add("latin1_swedish_ci");
    row.add("cp1252 West European");
    row.add("1");
    rows.add(row);
    // # CHARACTER_SET_NAME, DEFAULT_COLLATE_NAME, DESCRIPTION, MAXLEN
    // utf8mb4, utf8mb4_0900_ai_ci, UTF-8 Unicode, 4
    row = new ArrayList<>();
    row.add("utf8mb4");
    row.add("utf8mb4_0900_ai_ci");
    row.add("UTF-8 Unicode");
    row.add("4");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    query =
        "select * from `information_schema`.`collations` where character_set_name = 'latin1' order by `collation_name` asc";
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("COLLATION_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("CHARACTER_SET_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("ID", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("IS_DEFAULT", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("IS_COMPILED", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("SORTLEN", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("PAD_ATTRIBUTE", "VARCHAR(255)"));
    rows = new ArrayList<>();
    // # COLLATION_NAME, CHARACTER_SET_NAME, ID, IS_DEFAULT, IS_COMPILED, SORTLEN, PAD_ATTRIBUTE
    // latin1_swedish_ci, latin1, 8, Yes, Yes, 1, PAD SPACE
    row = new ArrayList<>();
    row.add("latin1_swedish_ci");
    row.add("latin1");
    row.add("8");
    row.add("Yes");
    row.add("Yes");
    row.add("1");
    row.add("PAD SPACE");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    // COLLATION_NAME, CHARACTER_SET_NAME, ID, IS_DEFAULT, IS_COMPILED, SORTLEN, PAD_ATTRIBUTE
    query =
        "select * from `information_schema`.`collations` where character_set_name = 'utf8mb4' order by `collation_name` asc";
    // # COLLATION_NAME, CHARACTER_SET_NAME, ID, IS_DEFAULT, IS_COMPILED, SORTLEN, PAD_ATTRIBUTE
    // utf8mb4_0900_ai_ci, utf8mb4, 255, Yes, Yes, 0, NO PAD
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("COLLATION_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("CHARACTER_SET_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("ID", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("IS_DEFAULT", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("IS_COMPILED", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("SORTLEN", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("PAD_ATTRIBUTE", "VARCHAR(255)"));
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("utf8mb4_0900_ai_ci");
    row.add("utf8mb4");
    row.add("255");
    row.add("Yes");
    row.add("Yes");
    row.add("0");
    row.add("NO PAD");
    rows.add(row);
    mockResults.put(query, new Pair<>(columns, rows));

    String pattern =
        "(?i)SELECT DISTINCT ROUTINE_SCHEMA, ROUTINE_NAME, PARAMS\\.PARAMETER FROM information_schema\\.ROUTINES LEFT JOIN \\( SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME, GROUP_CONCAT\\(CONCAT\\(DATA_TYPE, ' ', PARAMETER_NAME\\) ORDER BY ORDINAL_POSITION SEPARATOR ', '\\) PARAMETER, ROUTINE_TYPE FROM information_schema\\.PARAMETERS GROUP BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ROUTINE_TYPE \\) PARAMS ON ROUTINES\\.ROUTINE_SCHEMA = PARAMS\\.SPECIFIC_SCHEMA AND ROUTINES\\.ROUTINE_NAME = PARAMS\\.SPECIFIC_NAME AND ROUTINES\\.ROUTINE_TYPE = PARAMS\\.ROUTINE_TYPE WHERE ROUTINE_SCHEMA = '.+' ORDER BY ROUTINE_SCHEMA";
    // ROUTINE_SCHEMA	ROUTINE_NAME	PARAMETER
    columns = new ArrayList<>();
    columnNameStr = "ROUTINE_SCHEMA, ROUTINE_NAME, PARAMETER";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    pattern = "(?i)SELECT .+ FROM information_schema\\.routines WHERE routine_schema = '.+'";
    // SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, DATA_TYPE,
    // CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
    // DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, DTD_IDENTIFIER, ROUTINE_BODY,
    // ROUTINE_DEFINITION, EXTERNAL_NAME, EXTERNAL_LANGUAGE, PARAMETER_STYLE, IS_DETERMINISTIC,
    // SQL_DATA_ACCESS, SQL_PATH, SECURITY_TYPE, CREATED, LAST_ALTERED, SQL_MODE, ROUTINE_COMMENT,
    // DEFINER, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION
    columns = new ArrayList<>();
    columnNameStr =
        "SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, DTD_IDENTIFIER, ROUTINE_BODY, ROUTINE_DEFINITION, EXTERNAL_NAME, EXTERNAL_LANGUAGE, PARAMETER_STYLE, IS_DETERMINISTIC, SQL_DATA_ACCESS, SQL_PATH, SECURITY_TYPE, CREATED, LAST_ALTERED, SQL_MODE, ROUTINE_COMMENT, DEFINER, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    pattern = "(?i)SHOW INDEX FROM .*";
    // # Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality, Sub_part,
    // Packed, Null, Index_type, Comment, Index_comment, Visible, Expression
    // t_exam_paper, 0, PRIMARY, 1, id, A, 0, , , , BTREE, , , YES,
    columns = new ArrayList<>();
    columnNameStr =
        "Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality, Sub_part, Packed, Null, Index_type, Comment, Index_comment, Visible, Expression";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));
  }
}
