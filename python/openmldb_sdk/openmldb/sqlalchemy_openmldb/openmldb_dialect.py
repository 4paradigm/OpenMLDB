#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# fmt:off
import sys

from pathlib import Path

# add parent directory
sys.path.append(Path(__file__).parent.parent.as_posix())

import sqlalchemy
from ..dbapi import dbapi as module
from sqlalchemy.engine import default
from sqlalchemy import pool
from sqlalchemy.sql import compiler

# fmt:on

RESERVED_WORDS = set("select")


class OpenMLDBCompiler(compiler.SQLCompiler):

    def default_from(self):
        pass

    def visit_char_length_func(self, fn, **kw):
        pass

    def visit_table(self, table, asfrom=False, **kwargs):
        pass

    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        pass


class OpenMLDBIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(
        [
            'abs', 'all', 'allocate', 'allow', 'alter', 'and', 'any', 'are', 'array', 'as', 'asensitive',
            'asymmetric', 'at', 'atomic', 'authorization', 'avg', 'begin', 'between', 'bigint', 'binary',
            'bit', 'blob', 'boolean', 'both', 'by', 'call', 'called', 'cardinality', 'cascaded', 'case',
            'cast', 'ceil', 'ceiling', 'char', 'character', 'character_length', 'char_length', 'check',
            'clob', 'close', 'coalesce', 'collate', 'collect', 'column', 'commit', 'condition', 'connect',
            'constraint', 'convert', 'corr', 'corresponding', 'count', 'covar_pop', 'covar_samp', 'create',
            'cross', 'cube', 'cume_dist', 'current', 'current_catalog', 'current_date',
            'current_default_transform_group', 'current_path', 'current_role', 'current_schema', 'current_time',
            'current_timestamp', 'current_transform_group_for_type', 'current_user', 'cursor', 'cycle',
            'databases', 'date', 'day', 'deallocate', 'dec', 'decimal', 'declare', 'default', 'default_kw',
            'delete', 'dense_rank', 'deref', 'describe', 'deterministic', 'disallow', 'disconnect', 'distinct',
            'double', 'drop', 'dynamic', 'each', 'element', 'else', 'end', 'end_exec', 'escape', 'every', 'except',
            'exec', 'execute', 'exists', 'exp', 'explain', 'external', 'extract', 'false', 'fetch', 'files', 'filter',
            'first_value', 'float', 'floor', 'for', 'foreign', 'free', 'from', 'full', 'function', 'fusion', 'get',
            'global', 'grant', 'group', 'grouping', 'having', 'hold', 'hour', 'identity', 'if', 'import', 'in',
            'indicator', 'inner', 'inout', 'insensitive', 'insert', 'int', 'integer', 'intersect', 'intersection',
            'interval', 'into', 'is', 'jar', 'join', 'language', 'large', 'last_value', 'lateral', 'leading', 'left',
            'like', 'limit', 'ln', 'local', 'localtime', 'localtimestamp', 'lower', 'match', 'max', 'member', 'merge',
            'method', 'min', 'minute', 'mod', 'modifies', 'module', 'month', 'multiset', 'national', 'natural',
            'nchar', 'nclob', 'new', 'no', 'none', 'normalize', 'not', 'null', 'nullif', 'numeric', 'octet_length',
            'of', 'offset', 'old', 'on', 'only', 'open', 'or', 'order', 'out', 'outer', 'over', 'overlaps', 'overlay',
            'parameter', 'partition', 'percentile_cont', 'percentile_disc', 'percent_rank', 'position', 'power',
            'precision', 'prepare', 'primary', 'procedure', 'range', 'rank', 'reads', 'real', 'recursive', 'ref',
            'references', 'referencing', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2',
            'regr_slope', 'regr_sxx', 'regr_sxy', 'release', 'replace', 'result', 'return', 'returns', 'revoke',
            'right', 'rollback', 'rollup', 'row', 'rows', 'row_number', 'savepoint', 'schemas', 'scope', 'scroll',
            'search', 'second', 'select', 'sensitive', 'session_user', 'set', 'show', 'similar', 'smallint', 'some',
            'specific', 'specifictype', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning', 'sqrt', 'start', 'static',
            'stddev_pop', 'stddev_samp', 'submultiset', 'substring', 'sum', 'symmetric', 'system', 'system_user',
            'table', 'tables', 'tablesample', 'then', 'time', 'timestamp', 'timezone_hour', 'timezone_minute',
            'tinyint', 'to', 'trailing', 'translate', 'translation', 'treat', 'trigger', 'trim', 'true', 'uescape',
            'union', 'unique', 'unknown', 'unnest', 'update', 'upper', 'use', 'user', 'using', 'value', 'values',
            'varbinary', 'varchar', 'varying', 'var_pop', 'var_samp', 'when', 'whenever', 'where', 'width_bucket',
            'window', 'with', 'within', 'without', 'year'
        ]
    )

    def __init__(self, dialect):
        super(OpenMLDBIdentifierPreparer, self).__init__(dialect, initial_quote='`', final_quote='`')

    def format_drill_table(self, schema, isFile=True):
        pass


class OpenmldbDialect(default.DefaultDialect):
    name = "openmldb_dialect"
    driver = 'rest'
    dbapi = ""
    poolclass = pool.SingletonThreadPool
    returns_unicode_strings = True

    def __init__(self, **kw):
        default.DefaultDialect.__init__(self, **kw)

    @classmethod
    def dbapi(cls):
        return module

    def has_table(self, connection, table_name, schema=None):
        if schema is not None:
            raise Exception("schema unsupported in OpenMLDB")
        return table_name in connection.connection.cursor().get_all_tables()

    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of table names for `schema`."""
        return list(connection.connection.cursor().get_all_tables())

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        columns_info = [
            {
                'name': 'name',
                'type': sqlalchemy.String(),
                'nullable': False,
                'default': None,
                'primary_key': False,
                'autoincrement': False,
                'comment': 'Unique identifier for the row'
            }
        ]
        return columns_info
        """
        raise NotImplementedError()
