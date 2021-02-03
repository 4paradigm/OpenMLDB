import random

from fesql_param import sample_integer_config
from fesql_table import ColumnInfo, TypedExpr, ColumnKey, sample_expr, ColumnsPool, SubTable

from auto_case_tool import getRandomName
from fesql_const import SELECT_WINDOW_SQL, SELECT_JOIN_SQL, SIMPLE_SUB_SELECT


class QueryDesc:
    def __init__(self, text, columns):
        self.text = text
        self.columns = columns
        self.alias = None

def sample_window_project(mainTable,
                          window_defs:list, udf_defs, args,
                          downward=True, keep_index=True, is_sub_select=False):
    mainTable.init_table(args, window_defs, udf_defs, downward, keep_index)
    sql_string = SELECT_WINDOW_SQL
    sql_string = sql_string.replace("${WINDOW_EXPRS}", ", ".join([_.text for _ in mainTable.expressions]))
    if is_sub_select:
        sql_string = sql_string.replace("${INPUT_NAME}", mainTable.get_sub_sql())
    else:
        sql_string = sql_string.replace("${INPUT_NAME}", mainTable.name)
    sql_string = sql_string.replace("${WINDOW_DEF}", ",".join(_.get_window_def_string() for _ in window_defs))
    return QueryDesc(sql_string, mainTable.output_columns)

def sample_window_union_project(tables, window_defs:list, udf_defs, args, downward=True, keep_index=True, is_sub_select=False):
    mainTable = tables[0]
    mainTable.init_table(args, window_defs, udf_defs, downward, keep_index)
    for i in range(1,len(tables)):
        tables[i].order_columns = mainTable.order_columns
        tables[i].partition_columns = mainTable.partition_columns
        tables[i].normal_columns = mainTable.normal_columns
        tables[i].indexs = mainTable.indexs
    sql_string = SELECT_WINDOW_SQL
    sql_string = sql_string.replace("${WINDOW_EXPRS}", ", ".join([_.text for _ in mainTable.expressions]))
    if is_sub_select:
        sql_string = sql_string.replace("${INPUT_NAME}", mainTable.get_sub_sql())
    else:
        sql_string = sql_string.replace("${INPUT_NAME}", mainTable.name)
    sql_string = sql_string.replace("${WINDOW_DEF}", ",".join(_.get_window_string() for _ in window_defs))
    return QueryDesc(sql_string, mainTable.output_columns)

def sample_last_join_project(tables, window_defs:list, udf_defs, args, downward=True, keep_index=True, is_sub_select=False):
    mainTable = tables[0]
    for i in range(1,len(tables)):
        mainTable.join_table.append(tables[i])
    mainTable.init_join_table(args, window_defs, udf_defs, downward, keep_index)
    # SELECT_JOIN_SQL = "SELECT ${WINDOW_EXPRS} FROM ${INPUT_NAME} ${JOIN} WINDOW ${WINDOW_DEF}"
    sql_string = SELECT_JOIN_SQL
    sql_string = sql_string.replace("${WINDOW_EXPRS}", ", ".join([e.text for t in tables for e in t.expressions]))
    if is_sub_select:
        sql_string = sql_string.replace("${INPUT_NAME}", mainTable.get_sub_sql())
    else:
        sql_string = sql_string.replace("${INPUT_NAME}", mainTable.name)
    sql_string = sql_string.replace("${JOIN}", mainTable.get_join_def_string(is_sub_select))
    sql_string = sql_string.replace("${WINDOW_DEF}", ",".join(_.get_window_string() for _ in window_defs))
    return QueryDesc(sql_string, mainTable.output_columns)


def gen_query_desc(tables, window_defs:list, udf_defs, args, downward=True, keep_index=True, is_sub_select=False):
    sub_query_type = sample_integer_config(args.sub_query_type)
    if len(tables) == 1:
        sub_query_type = 0
    # sub_query_type = 2
    if sub_query_type == 0:
        table = tables[0]
        sub_query = sample_window_project(table,
            window_defs=window_defs, udf_defs=udf_defs,
            args=args, downward=downward, keep_index=keep_index, is_sub_select=is_sub_select)
    elif sub_query_type == 1:
        for window_def in window_defs:
            window_def.window_type = sample_integer_config(args.window_type)
            if window_def.window_type == 1:
                for i in range(1, len(tables)):
                    window_def.tables.add(tables[i].name)
        sub_query = sample_window_union_project(
            tables, window_defs=window_defs, udf_defs=udf_defs,
            args=args, downward=downward, keep_index=keep_index, is_sub_select=is_sub_select)
    elif sub_query_type == 2:
        sub_query = sample_last_join_project(
            tables, window_defs=window_defs, udf_defs=udf_defs,
            args=args, downward=downward, keep_index=keep_index, is_sub_select=is_sub_select)
    return sub_query

def sample_select_project(tables, window_defs:list, udf_defs, args, downward=True, keep_index=True):
    pass

def gen_simple_subselect(sub_query:QueryDesc):
    # SELECT ${EXPRS} FROM (${SUB_SELECT}) AS ${SUB_ALIAS}
    sql_string = SIMPLE_SUB_SELECT
    sql_string = sql_string.replace("${EXPRS}", ", ".join([e.name for e in sub_query.columns]))
    sql_string = sql_string.replace("${SUB_SELECT}", sub_query.text)
    sql_string = sql_string.replace("${SUB_ALIAS}", sub_query.alias)
    query_desc = QueryDesc(sql_string, sub_query.columns)
    return query_desc

def gen_subtable(tables , window_defs:list, udf_defs, args, downward=True, keep_index=True, is_sub_select=False):
    # sub_query_type = sample_integer_config(args.sub_query_type)
    # sub_query_type = 2
    sub_query = gen_query_desc(tables, window_defs, udf_defs, args, downward=downward, keep_index=keep_index, is_sub_select=is_sub_select)
    main_table = tables[0]
    subTable = SubTable(args)
    subTable.name = getRandomName(prefix="sub_")
    subTable.partition_columns = main_table.partition_columns
    subTable.order_columns = main_table.order_columns
    subTable.id_column = main_table.id_column
    subTable.indexs = main_table.indexs
    subTable.sql = sub_query.text
    exist_cloumns = [subTable.id_column]
    exist_cloumns.extend(subTable.partition_columns)
    exist_cloumns.extend(subTable.order_columns)
    exist_column_names = [c.name for c in exist_cloumns]
    for c in sub_query.columns:
        if c.name not in exist_column_names:
            subTable.normal_columns.append(c)
    return subTable

def sample_subselect_project(tables, window_defs:list, udf_defs, args, downward=True, keep_index=True):
    sub_query = gen_query_desc(tables, window_defs, udf_defs, args, downward=downward, keep_index=keep_index, is_sub_select=True)
    sub_query.alias = "sub_0"
    query_desc = gen_simple_subselect(sub_query)
    # sub_tables = []
    # sub_table_num = 2
    # for i in range(sub_table_num):
    #     subTable = gen_subtable(tables, window_defs, udf_defs, args, downward=downward, keep_index=keep_index, is_sub_select=False)
    #     sub_tables.append(subTable)
    # query_desc = gen_query_desc(sub_tables, window_defs, udf_defs, args, False, keep_index=keep_index, is_sub_select=True)
    return query_desc
