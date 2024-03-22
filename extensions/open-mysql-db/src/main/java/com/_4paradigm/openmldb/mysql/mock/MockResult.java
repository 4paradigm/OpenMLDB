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

  public static Map<String, String> mockSessionStatusVariables = new HashMap<>();
  public static Map<String, String> mockSessionVariablesVariables = new HashMap<>();

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

    query = "show charset";
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

    query = "show variables like 'lower_case_table_names'";
    // Variable_name	Value
    // lower_case_table_names	2
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Variable_name", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Value", "VARCHAR(255)"));
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("lower_case_table_names");
    row.add("2");
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

    mockSessionStatusVariables.put(
        "sql_mode",
        "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION");
    mockSessionStatusVariables.put("Ssl_cipher", "TLS_AES_256_GCM_SHA384");
    mockSessionStatusVariables.put("version_comment", MySqlListener.VERSION_COMMENT);
    mockSessionStatusVariables.put("version", MySqlListener.VERSION);
    for (String sessionVariable : MockResult.mockSessionStatusVariables.keySet()) {
      query = "show session status like '" + sessionVariable.toLowerCase() + "'";
      columns = new ArrayList<>();
      columns.add(new QueryResultColumn("Variable_name", "VARCHAR(255)"));
      columns.add(new QueryResultColumn("Value", "VARCHAR(255)"));
      rows = new ArrayList<>();
      row = new ArrayList<>();
      row.add(sessionVariable);
      row.add(mockSessionStatusVariables.get(sessionVariable));
      rows.add(row);
      mockResults.put(query, new Pair<>(columns, rows));
    }

    mockSessionVariablesVariables.put(
        "sql_mode",
        "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION");
    mockSessionVariablesVariables.put("version_comment", MySqlListener.VERSION_COMMENT);
    mockSessionVariablesVariables.put("version", MySqlListener.VERSION);
    // Variable_name	Value
    // version_compile_os	macos12
    mockSessionVariablesVariables.put("version_compile_os", "");
    mockSessionVariablesVariables.put("offline_mode", "OFF");
    mockSessionVariablesVariables.put("wait_timeout", "28800");
    mockSessionVariablesVariables.put("interactive_timeout", "28800");
    mockSessionVariablesVariables.put("lower_case_table_names", "2");
    for (String sessionVariable : MockResult.mockSessionVariablesVariables.keySet()) {
      query = "show session variables like '" + sessionVariable.toLowerCase() + "'";
      columns = new ArrayList<>();
      columns.add(new QueryResultColumn("Variable_name", "VARCHAR(255)"));
      columns.add(new QueryResultColumn("Value", "VARCHAR(255)"));
      rows = new ArrayList<>();
      row = new ArrayList<>();
      row.add(sessionVariable);
      row.add(mockSessionVariablesVariables.get(sessionVariable));
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

    query = "show status";
    // Variable_name	Value
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("Variable_name", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("Value", "VARCHAR(255)"));
    rows = new ArrayList<>();
    mockResults.put(query, new Pair<>(columns, rows));

    query =
        "select query_id, sum(duration) as sum_duration from information_schema.profiling group by query_id";
    // QUERY_ID	SUM_DURATION
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("QUERY_ID", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("SUM_DURATION", "VARCHAR(255)"));
    rows = new ArrayList<>();
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

    query = "select word from information_schema.keywords where reserved=1 order by word";
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("WORD", "VARCHAR(255)"));
    rows = new ArrayList<>();
    String keyWords =
        "ACCESSIBLE,ADD,ALL,ALTER,ANALYZE,AND,AS,ASC,ASENSITIVE,BEFORE,BETWEEN,BIGINT,BINARY,BLOB,BOTH,BY,CALL,CASCADE,CASE,CHANGE,CHAR,CHARACTER,CHECK,COLLATE,COLUMN,CONDITION,CONSTRAINT,CONTINUE,CONVERT,CREATE,CROSS,CUBE,CUME_DIST,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DEC,DECIMAL,DECLARE,DEFAULT,DELAYED,DELETE,DENSE_RANK,DESC,DESCRIBE,DETERMINISTIC,DISTINCT,DISTINCTROW,DIV,DOUBLE,DROP,DUAL,EACH,ELSE,ELSEIF,EMPTY,ENCLOSED,ESCAPED,EXCEPT,EXISTS,EXIT,EXPLAIN,FALSE,FETCH,FIRST_VALUE,FLOAT,FLOAT4,FLOAT8,FOR,FORCE,FOREIGN,FROM,FULLTEXT,FUNCTION,GENERATED,GET,GRANT,GROUP,GROUPING,GROUPS,HAVING,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,IN,INDEX,INFILE,INNER,INOUT,INSENSITIVE,INSERT,INT,INT1,INT2,INT3,INT4,INT8,INTEGER,INTERVAL,INTO,IO_AFTER_GTIDS,IO_BEFORE_GTIDS,IS,ITERATE,JOIN,JSON_TABLE,KEY,KEYS,KILL,LAG,LAST_VALUE,LATERAL,LEAD,LEADING,LEAVE,LEFT,LIKE,LIMIT,LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER_BIND,MASTER_SSL_VERIFY_SERVER_CERT,MATCH,MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODIFIES,NATURAL,NO_WRITE_TO_BINLOG,NOT,NTH_VALUE,NTILE,NULL,NUMERIC,OF,ON,OPTIMIZE,OPTIMIZER_COSTS,OPTION,OPTIONALLY,OR,ORDER,OUT,OUTER,OUTFILE,OVER,PARTITION,PERCENT_RANK,PRECISION,PRIMARY,PROCEDURE,PURGE,RANGE,RANK,READ,READ_WRITE,READS,REAL,RECURSIVE,REFERENCES,REGEXP,RELEASE,RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RETURN,REVOKE,RIGHT,RLIKE,ROW,ROW_NUMBER,ROWS,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SELECT,SENSITIVE,SEPARATOR,SET,SHOW,SIGNAL,SMALLINT,SPATIAL,SPECIFIC,SQL,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SQLEXCEPTION,SQLSTATE,SQLWARNING,SSL,STARTING,STORED,STRAIGHT_JOIN,SYSTEM,TABLE,TERMINATED,THEN,TINYBLOB,TINYINT,TINYTEXT,TO,TRAILING,TRIGGER,TRUE,UNDO,UNION,UNIQUE,UNLOCK,UNSIGNED,UPDATE,USAGE,USE,USING,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VALUES,VARBINARY,VARCHAR,VARCHARACTER,VARYING,VIRTUAL,WHEN,WHERE,WHILE,WINDOW,WITH,WRITE,XOR,YEAR_MONTH,ZEROFILL";
    for (String keyWord : keyWords.split(",")) {
      row = new ArrayList<>();
      row.add(keyWord);
      rows.add(row);
    }
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

    // mock for dbeaver
    // SELECT * FROM information_schema.TABLES t
    // WHERE
    //	t.TABLE_SCHEMA = 'information_schema'
    //	AND t.TABLE_NAME = 'CHECK_CONSTRAINTS'
    pattern =
        "(?i)SELECT \\* FROM information_schema\\.TABLES t\\s+WHERE\\s+t\\.TABLE_SCHEMA = 'information_schema'\\s+AND t\\.TABLE_NAME = 'CHECK_CONSTRAINTS'";
    // TABLE_CATALOG	TABLE_SCHEMA	TABLE_NAME	TABLE_TYPE	ENGINE	VERSION	ROW_FORMAT	TABLE_ROWS
    //	AVG_ROW_LENGTH	DATA_LENGTH	MAX_DATA_LENGTH	INDEX_LENGTH	DATA_FREE	AUTO_INCREMENT	CREATE_TIME
    //	UPDATE_TIME	CHECK_TIME	TABLE_COLLATION	CHECKSUM	CREATE_OPTIONS	TABLE_COMMENT
    // def	information_schema	CHECK_CONSTRAINTS	SYSTEM VIEW		10		0	0	0	0	0	0		2022-06-26 19:58:21
    columns = new ArrayList<>();
    columnNameStr =
        "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, VERSION, ROW_FORMAT, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, AUTO_INCREMENT, CREATE_TIME, UPDATE_TIME, CHECK_TIME, TABLE_COLLATION, CHECKSUM, CREATE_OPTIONS, TABLE_COMMENT";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    row = new ArrayList<>();
    row.add("def");
    row.add("information_schema");
    row.add("CHECK_CONSTRAINTS");
    row.add("SYSTEM VIEW");
    row.add(null);
    row.add("10");
    row.add(null);
    row.add("0");
    row.add("0");
    row.add("0");
    row.add("0");
    row.add("0");
    row.add("0");
    row.add(null);
    row.add("2022-06-26 19:58:21");
    row.add(null);
    row.add(null);
    row.add(null);
    row.add(null);
    row.add("");
    row.add("");
    rows.add(row);
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

    // SELECT TABLE_NAME, CHECK_OPTION, IS_UPDATABLE, SECURITY_TYPE, DEFINER FROM
    // INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'demo_db' ORDER BY TABLE_NAME ASC
    pattern =
        "(?i)SELECT .* FROM INFORMATION_SCHEMA\\.VIEWS WHERE TABLE_SCHEMA = '.+' ORDER BY TABLE_NAME ASC";
    // TABLE_NAME	CHECK_OPTION	IS_UPDATABLE	SECURITY_TYPE	DEFINER
    columns = new ArrayList<>();
    columnNameStr = "TABLE_NAME, CHECK_OPTION, IS_UPDATABLE, SECURITY_TYPE, DEFINER";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    // SELECT SUM((DATA_LENGTH+INDEX_LENGTH))
    // FROM INFORMATION_SCHEMA.TABLES
    // WHERE TABLE_SCHEMA='demo_db'
    pattern =
        "(?i)SELECT SUM\\(\\(DATA_LENGTH\\+INDEX_LENGTH\\)\\)\\s+FROM INFORMATION_SCHEMA\\.TABLES\\s+WHERE TABLE_SCHEMA\\s*=\\s*'.+'";
    // SUM((DATA_LENGTH+INDEX_LENGTH))
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("SUM((DATA_LENGTH+INDEX_LENGTH))", "VARCHAR(255)"));
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    // SELECT * FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='xzs' AND
    // TABLE_NAME='t_exam_paper' ORDER BY TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX
    pattern = "(?i)SELECT \\* FROM information_schema\\.STATISTICS.*";
    // TABLE_CATALOG	TABLE_SCHEMA	TABLE_NAME	NON_UNIQUE	INDEX_SCHEMA	INDEX_NAME	SEQ_IN_INDEX
    //	COLUMN_NAME	COLLATION	CARDINALITY	SUB_PART	PACKED	NULLABLE	INDEX_TYPE	COMMENT	INDEX_COMMENT
    //	IS_VISIBLE	EXPRESSION
    // def	xzs	t_exam_paper	0	xzs	PRIMARY	1	id	A	0				BTREE			YES
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("TABLE_CATALOG", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("TABLE_SCHEMA", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("TABLE_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("NON_UNIQUE", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("INDEX_SCHEMA", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("INDEX_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("SEQ_IN_INDEX", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("COLUMN_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("COLLATION", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("CARDINALITY", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("SUB_PART", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("PACKED", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("NULLABLE", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("INDEX_TYPE", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("COMMENT", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("INDEX_COMMENT", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("IS_VISIBLE", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("EXPRESSION", "VARCHAR(255)"));
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    // SELECT kc.CONSTRAINT_NAME,kc.TABLE_NAME,kc.COLUMN_NAME,kc.ORDINAL_POSITION
    // FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc WHERE kc.TABLE_SCHEMA='demo_db' AND
    // kc.REFERENCED_TABLE_NAME IS NULL AND kc.TABLE_NAME='demo_table1'
    // ORDER BY kc.CONSTRAINT_NAME,kc.ORDINAL_POSITION
    pattern = "(?i)SELECT .*\\s+FROM INFORMATION_SCHEMA\\.KEY_COLUMN_USAGE.*\\s+ORDER BY.*";
    // CONSTRAINT_NAME	TABLE_NAME	COLUMN_NAME	ORDINAL_POSITION
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("CONSTRAINT_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("TABLE_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("COLUMN_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("ORDINAL_POSITION", "VARCHAR(255)"));
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    // SELECT DISTINCT A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT,NULL AS PKTABLE_SCHEM,
    // A.REFERENCED_TABLE_NAME AS PKTABLE_NAME, A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME,
    // A.TABLE_SCHEMA AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, A.TABLE_NAME AS FKTABLE_NAME,
    // A.COLUMN_NAME AS FKCOLUMN_NAME, A.ORDINAL_POSITION AS KEY_SEQ,CASE WHEN
    // R.UPDATE_RULE='CASCADE' THEN 0 WHEN R.UPDATE_RULE='SET NULL' THEN 2 WHEN R.UPDATE_RULE='SET
    // DEFAULT' THEN 4 WHEN R.UPDATE_RULE='RESTRICT' THEN 1 WHEN R.UPDATE_RULE='NO ACTION' THEN 1
    // ELSE 1 END  AS UPDATE_RULE,CASE WHEN R.DELETE_RULE='CASCADE' THEN 0 WHEN R.DELETE_RULE='SET
    // NULL' THEN 2 WHEN R.DELETE_RULE='SET DEFAULT' THEN 4 WHEN R.DELETE_RULE='RESTRICT' THEN 1
    // WHEN R.DELETE_RULE='NO ACTION' THEN 1 ELSE 1 END  AS DELETE_RULE, A.CONSTRAINT_NAME AS
    // FK_NAME, R.UNIQUE_CONSTRAINT_NAME AS PK_NAME,7 AS DEFERRABILITY FROM
    // INFORMATION_SCHEMA.KEY_COLUMN_USAGE A JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B USING
    // (CONSTRAINT_NAME, TABLE_NAME) JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R ON
    // (R.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND R.TABLE_NAME = B.TABLE_NAME AND
    // R.CONSTRAINT_SCHEMA = B.TABLE_SCHEMA) WHERE B.CONSTRAINT_TYPE = 'FOREIGN KEY' AND
    // A.TABLE_SCHEMA = 'demo_db' AND A.TABLE_NAME='demo_table1' AND A.REFERENCED_TABLE_SCHEMA IS
    // NOT NULL ORDER BY A.REFERENCED_TABLE_SCHEMA, A.REFERENCED_TABLE_NAME, A.ORDINAL_POSITION
    pattern =
        "(?i)SELECT DISTINCT .+\\.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT,NULL AS PKTABLE_SCHEM, .+\\.REFERENCED_TABLE_NAME AS PKTABLE_NAME, .+\\.REFERENCED_COLUMN_NAME.*\\s+FROM INFORMATION_SCHEMA\\.KEY_COLUMN_USAGE.*";
    // PKTABLE_CAT PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME
    //	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("PKTABLE_CAT", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("PKTABLE_SCHEM", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("PKTABLE_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("PKCOLUMN_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("FKTABLE_CAT", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("FKTABLE_SCHEM", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("FKTABLE_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("FKCOLUMN_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("KEY_SEQ", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("UPDATE_RULE", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("DELETE_RULE", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("FK_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("PK_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("DEFERRABILITY", "VARCHAR(255)"));
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    // SELECT cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE, tc.TABLE_NAME
    // FROM (SELECT CONSTRAINT_NAME, CHECK_CLAUSE
    // FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
    // WHERE CONSTRAINT_SCHEMA = 'xzs'
    // ORDER BY CONSTRAINT_NAME) cc,
    // (SELECT TABLE_NAME, CONSTRAINT_NAME
    // FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    // WHERE TABLE_SCHEMA = 'xzs' AND TABLE_NAME='t_exam_paper') tc
    // WHERE cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    // ORDER BY cc.CONSTRAINT_NAME
    pattern =
        "(?i)(?s)SELECT .*CONSTRAINT_NAME,.*CHECK_CLAUSE,.*TABLE_NAME.*FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS.*FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS.*";
    // CONSTRAINT_NAME	CHECK_CLAUSE	TABLE_NAME
    columns = new ArrayList<>();
    columns.add(new QueryResultColumn("CONSTRAINT_NAME", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("CHECK_CLAUSE", "VARCHAR(255)"));
    columns.add(new QueryResultColumn("TABLE_NAME", "VARCHAR(255)"));
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    // SELECT st.* FROM performance_schema.events_waits_history_long st WHERE st.nesting_event_id =
    // 0
    pattern = "(?i)(?s)SELECT .*FROM performance_schema.events_waits_history_long.*WHERE.*";
    // THREAD_ID, EVENT_ID, END_EVENT_ID, EVENT_NAME, SOURCE, TIMER_START, TIMER_END, TIMER_WAIT,
    // SPINS, OBJECT_SCHEMA, OBJECT_NAME, INDEX_NAME, OBJECT_TYPE, OBJECT_INSTANCE_BEGIN,
    // NESTING_EVENT_ID, NESTING_EVENT_TYPE, OPERATION, NUMBER_OF_BYTES, FLAGS
    columns = new ArrayList<>();
    columnNameStr =
        "THREAD_ID, EVENT_ID, END_EVENT_ID, EVENT_NAME, SOURCE, TIMER_START, TIMER_END, TIMER_WAIT, SPINS, OBJECT_SCHEMA, OBJECT_NAME, INDEX_NAME, OBJECT_TYPE, OBJECT_INSTANCE_BEGIN, NESTING_EVENT_ID, NESTING_EVENT_TYPE, OPERATION, NUMBER_OF_BYTES, FLAGS";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    // SELECT st.* FROM performance_schema.events_statements_current st JOIN
    // performance_schema.threads thr ON thr.thread_id = st.thread_id WHERE thr.processlist_id =
    // -901422372
    pattern = "(?i)(?s)SELECT .*FROM performance_schema.events_statements_current.*WHERE.*";
    // THREAD_ID, EVENT_ID, END_EVENT_ID, EVENT_NAME, SOURCE, TIMER_START, TIMER_END, TIMER_WAIT,
    // LOCK_TIME, SQL_TEXT, DIGEST, DIGEST_TEXT, CURRENT_SCHEMA, OBJECT_TYPE, OBJECT_SCHEMA,
    // OBJECT_NAME, OBJECT_INSTANCE_BEGIN, MYSQL_ERRNO, RETURNED_SQLSTATE, MESSAGE_TEXT, ERRORS,
    // WARNINGS, ROWS_AFFECTED, ROWS_SENT, ROWS_EXAMINED, CREATED_TMP_DISK_TABLES,
    // CREATED_TMP_TABLES, SELECT_FULL_JOIN, SELECT_FULL_RANGE_JOIN, SELECT_RANGE,
    // SELECT_RANGE_CHECK, SELECT_SCAN, SORT_MERGE_PASSES, SORT_RANGE, SORT_ROWS, SORT_SCAN,
    // NO_INDEX_USED, NO_GOOD_INDEX_USED, NESTING_EVENT_ID, NESTING_EVENT_TYPE, NESTING_EVENT_LEVEL,
    // STATEMENT_ID, CPU_TIME, EXECUTION_ENGINE
    columns = new ArrayList<>();
    columnNameStr =
        "THREAD_ID, EVENT_ID, END_EVENT_ID, EVENT_NAME, SOURCE, TIMER_START, TIMER_END, TIMER_WAIT, LOCK_TIME, SQL_TEXT, DIGEST, DIGEST_TEXT, CURRENT_SCHEMA, OBJECT_TYPE, OBJECT_SCHEMA, OBJECT_NAME, OBJECT_INSTANCE_BEGIN, MYSQL_ERRNO, RETURNED_SQLSTATE, MESSAGE_TEXT, ERRORS, WARNINGS, ROWS_AFFECTED, ROWS_SENT, ROWS_EXAMINED, CREATED_TMP_DISK_TABLES, CREATED_TMP_TABLES, SELECT_FULL_JOIN, SELECT_FULL_RANGE_JOIN, SELECT_RANGE, SELECT_RANGE_CHECK, SELECT_SCAN, SORT_MERGE_PASSES, SORT_RANGE, SORT_ROWS, SORT_SCAN, NO_INDEX_USED, NO_GOOD_INDEX_USED, NESTING_EVENT_ID, NESTING_EVENT_TYPE, NESTING_EVENT_LEVEL, STATEMENT_ID, CPU_TIME, EXECUTION_ENGINE";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));

    // SELECT st.* FROM performance_schema.events_stages_history_long st WHERE st.nesting_event_id =
    // 0
    pattern = "(?i)(?s)SELECT .*FROM performance_schema.events_stages_history_long.*WHERE.*";
    // # THREAD_ID, EVENT_ID, END_EVENT_ID, EVENT_NAME, SOURCE, TIMER_START, TIMER_END, TIMER_WAIT,
    // WORK_COMPLETED, WORK_ESTIMATED, NESTING_EVENT_ID, NESTING_EVENT_TYPE
    columns = new ArrayList<>();
    columnNameStr =
        "THREAD_ID, EVENT_ID, END_EVENT_ID, EVENT_NAME, SOURCE, TIMER_START, TIMER_END, TIMER_WAIT, WORK_COMPLETED, WORK_ESTIMATED, NESTING_EVENT_ID, NESTING_EVENT_TYPE";
    for (String columnName : columnNameStr.split(", ")) {
      columns.add(new QueryResultColumn(columnName, "VARCHAR(255)"));
    }
    rows = new ArrayList<>();
    mockPatternResults.put(pattern, new Pair<>(columns, rows));
  }
}
