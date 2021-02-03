import datetime

# common process exitcode for sql test
SQL_ENGINE_SUCCESS = 0
SQL_ENGINE_CASE_ERROR = 1
SQL_ENGINE_COMPILE_ERROR = 2
SQL_ENGINE_RUN_ERROR = 3

current_time = datetime.datetime.now()

#sql temp
WINDOW_SQL = "{} AS (PARTITION BY {} ORDER BY {} {})"
# SELECT_WINDOW_SQL = "SELECT {} FROM {} WINDOW {}"
SELECT_WINDOW_SQL = "SELECT ${WINDOW_EXPRS} FROM ${INPUT_NAME} WINDOW ${WINDOW_DEF}"

WINDOW_UNION_SQL = "{} AS (UNION {} PARTITION BY {} ORDER BY {} {} {})"

SELECT_JOIN_SQL = "SELECT ${WINDOW_EXPRS} FROM ${INPUT_NAME} ${JOIN} WINDOW ${WINDOW_DEF}"

LAST_JOIN_SQL = "LAST JOIN ${TABLE_NAME} ORDER BY ${ORDER_COLUMN} ON ${JOIN_EXPR}"

SIMPLE_SUB_SELECT = "SELECT ${EXPRS} FROM (${SUB_SELECT}) AS ${SUB_ALIAS}"

# mapping from function name to literal op
BUILTIN_OP_DICT = {
    "add": "+",
    "minus": "-",
    "multiply": "*",
    "div": "DIV",
    "fdiv": "/",
    "mod": "%",
    "and": "AND",
    "or": "OR",
    "xor": "XOR",
    "not": "NOT",
    "eq": "=",
    "neq": "!=",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">="
}

LAST_JOIN_OP = ["=", "<", "<=", ">", ">="]

PRIMITIVE_TYPES = [
    "bool",
    "int16",
    "int32",
    "int64",
    "float",
    "double",
    "date",
    "timestamp",
    "string"
]

VALID_PARTITION_TYPES = [
    "int64",
    "int32",
    "string",
    "timestamp",
    "date"
]

VALID_ORDER_TYPES = [
    "int64",
    "timestamp"
]

# sql preserved names which should be wrapped in ``
SQL_PRESERVED_NAMES = {
    "string",
}