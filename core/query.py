'''Query'''
# pylint: disable=unused-variable
# pylint: disable=line-too-long
# pylint: disable=wrong-import-order
# pylint: disable=broad-exception-caught
# pylint: disable=unused-import
# pylint: disable=invalid-name
# pylint: disable=trailing-whitespace
import os
import sys
import datetime
import re
from sqlalchemy import Table
from sqlalchemy.sql import text
from core.access import Access

class Query:
    '''Query'''
    def __init__(self, conf, params, db, i18n):
        self.conf = conf
        self.params = params
        self.db = db
        self.i18n = i18n
    #CHECK TABLE PERMISSIONS
    def check_table_access(self, table, permissions, crud_action):
        '''check table access'''
        user = self.params.get('user')
        #print(table, crud_action, permissions[table].get(crud_action))
        if user.get('role_id') != 1:
            if not permissions.get(table):
                return {'success': False, 'msg': self.i18n('no-table-access', table = table)}
            elif permissions[table].get(crud_action) is False \
                or permissions[table].get(crud_action) == 0 \
                or not permissions[table].get(crud_action):
                return {'success': False, 'msg': self.i18n('no-table-action-access', table = table, action = crud_action)}
        return {'success': True}   
    def re_fn(self, expr, item):
        '''regex funcition'''
        reg = re.compile(expr, re.I)
        return reg.search(item) is not None
    def set_filters(self, sql, filters):
        '''apply query filters'''
        for _filter in filters:
            patt = re.compile(f'@{_filter}', re.I)
            _str = "''"
            if isinstance(filters[_filter], list):
                _str = "','".join([str(v) for v in filters[_filter]])
                _str = f"'{_str}'"
            elif isinstance(filters[_filter], (int, float)):
                _str = f"{str(filters[_filter])}"
            else:
                _str = f"'{str(filters[_filter])}'"
            sql = re.sub(patt, _str, sql)
        return sql
    def get_query(self, _query, _extra_conds = {}, _main_table_fields = [], user = {}):# pylint: disable = dangerous-default-value
        '''get query'''
        _table = None
        _tables = []
        _query_parts = {
            'select': '',
            'from': '',
            'join': '',
            'where': '',
            'group_by': '',
            'order_by': ''
        }
        for field in _query:
            _tables.append(field.get("table"))
            if field.get('ignore'):
                pass
            else:
                _label = f' AS "{field.get("field")}"'
                _field = f'"{field.get("table")}"."{field.get("field")}"'
                if field.get('label'):
                    _label = f' AS "{field.get("label")}"'
                if field.get('field') == "*":
                    _label = ''
                    _field = f'"{field.get("table")}".{field.get("field")}'
                if field.get('formula'):
                    _field = f'{field.get("field")}'        
                if field.get('hidden'):
                    pass
                elif field.get('func'):
                    _query_parts['select'] += f'{", " if _query_parts.get("select") else ""}{field.get("func")}({_field}){_label}'
                else:
                    _query_parts['select'] += f'{", " if _query_parts.get("select") else ""}{_field}{_label}'
                if not _query_parts.get("from") and field.get('table'):
                    _table = field.get('table')
                    _query_parts['from'] += f'{" " if _query_parts.get("from") else ""}"{field.get("table")}"'
                if field.get('join'):
                    _query_parts['join'] += f'''{" " if _query_parts.get("join") else ""}LEFT OUTER JOIN "{field.get("table")}" ON "{field.get("table")}"."{field.get("field")}" = {field.get("join")}'''
                if field.get('where_cond'):
                    where_val = f'{field.get("where_val")}'
                    skip = False
                    if field.get('where_cond') in ['LIKE', 'NOT LIKE']:
                        if where_val.find('%') != -1:
                            where_val = f'{where_val}'
                        else:
                            where_val = f'''\'%{field.get("where_val")}%\''''
                            where_val = f'''%{field.get("where_val")}%'''
                    elif field.get('where_cond') in ['IN', 'NOT IN']:
                        _ins = where_val.split(';')
                        if len(_ins) > 1:
                            where_val = f'''({"', '".join(_ins)})'''
                        else:
                            skip = True
                    elif field.get('where_cond') in ['BETWEEN', 'NOT BETWEEN']:
                        _ins = where_val.split(';')
                        if len(_ins) == 2:
                            where_val = f'{" AND ".join(_ins)}'
                        else:
                            skip = True
                    elif field.get('where_cond') in ['IS NULL', 'IS NOT NULL', 'IS TRUE', 'IS FALSE']:
                        where_val = ''
                    if not where_val and field.get('where_cond') not in ['IS NULL', 'IS NOT NULL', 'IS TRUE', 'IS FALSE']:
                        skip = True
                    if skip is False:
                        _is_string = ''
                        if not isinstance(where_val, (int, float, bool)) and where_val != '':
                            _is_string = '\''
                        _query_parts['where'] += f'{" AND " if _query_parts.get("where") else ""}{_field} {field.get("where_cond")} {_is_string}{where_val}{_is_string}'
                if field.get('group_by') and not field.get('hidden'):
                    _query_parts['group_by'] += f'{", " if _query_parts.get("group_by") else ""}{_field}'
                if field.get('order_by'):
                    _query_parts['order_by'] += f'{", " if _query_parts.get("order_by") else ""}{_field} {field.get("order_by")}'
        if len(_extra_conds.keys()) > 0:
            _conds = []
            for _tbl in _extra_conds:
                if _tbl in _tables:
                    _conds.append(_extra_conds.get(_tbl))
            if len(_conds > 0):
                _aux_compil_conds = ' AND '.join(_conds)
                if 'user_id' in _main_table_fields:
                    _query_parts['where'] += f'''{" AND " if _query_parts.get("where") else ""}("{_table}"."user_id" = {user.get('user_id')} OR ({_aux_compil_conds}))'''
                else:
                    _query_parts['where'] += f'{" AND " if _query_parts.get("where") else ""}({_aux_compil_conds})'
        return ''.join([
            f'SELECT {_query_parts.get("select")}', 
            f'\nFROM {_query_parts.get("from")}',
            f'\n{_query_parts.get("join")}' if _query_parts.get("join") else "",
            f"\nWHERE {_query_parts.get('where')}" if _query_parts.get("where") else "",
            f"\nGROUP BY {_query_parts.get('group_by')}" if _query_parts.get("group_by") else "",
            f"\nORDER BY {_query_parts.get('order_by')}" if _query_parts.get("order_by") else ""
        ])
    async def exec_query(self, engine, _sql_query):
        '''exec query'''
        try:
            #if isinstance(_sql_query, str):
            #    return {'success': False, 'msg': self.i18n('invalid-query', query = str(_sql_query))}
            patt = re.compile(r'CREATE.*TABLE|UPDATE.*TABLE|DROP.*|INSERT.*INTO|DELETE|ALTER.*TABLE|UPSERT.*')
            _match = re.findall(patt, _sql_query)
            if len(_match) > 0:
                return {'success': False, 'msg': self.i18n('query-not-allowed', query = str(_sql_query), match = '; '.join(_match))}
            if self.params.get('only_return_str') is True:
                return {'success': True, 'msg': self.i18n('success'), 'sql': _sql_query}
            with engine.connect() as conn:
                if engine.driver in ('pysqlite', 'sqlite'):
                    cache_size = self.conf.get('SQLITE_CACHE_SIZE', -2 * 1024 * 1024)
                    conn.execute(text(f'PRAGMA cache_size = {str(cache_size)}'))
                    busy_timeout = self.conf.get('SQLITE_BUSY_TIMEOUT', 60 * 1000) # 60s / 1m
                    conn.execute(text(f'PRAGMA busy_timeout = {busy_timeout}'))
                _sql_query_total = None
                if not self.params['data'].get('limit'):
                    pass
                elif self.params['data'].get('limit') != -1:
                    limit = self.params['data'].get('limit', 10)
                    offset = self.params['data'].get('offset', 0)
                    _sql_query_total = f'SELECT COUNT(*) AS "N_ROWS" FROM({_sql_query}) AS "T"'
                    # if engine.driver in ('pysqlite', 'sqlite', 'mysql', 'pymysql', 'postgres', 'pypostgres', 'pyscopg', 'duckdb', 'duckdb_engine):
                    _sql_query = f'{_sql_query} LIMIT {str(limit)} OFFSET {str(offset)}'
                # print('EXEC QUERY: ', self.params['data'].get('limit'), engine.driver, _sql_query)
                res = conn.execute(text(_sql_query))
                _res_data = res.mappings().all()
                res.close()
                data = []
                cols = []
                for row in _res_data:
                    __d = {}
                    if len(cols) == 0:                
                        cols = list(row)
                    for column in cols:
                        __d[column] = row[column]
                        if isinstance(__d[column], (datetime.datetime, datetime.date)):
                            __d[column] = __d[column].isoformat()
                    data.append(__d)
                _n_rows = len(data)
                if _sql_query_total:
                    res = conn.execute(text(_sql_query_total))
                    _res_total = res.mappings().all()
                    res.close()
                    _n_rows = _res_total[0].get('N_ROWS', 0)
                conn.close()
                return {'success': True, 'msg': self.i18n('success'), 'data': data, 'cols': cols, 'n_rows': _n_rows}
        except Exception as _err:# pylint: disable=broad-exception-caught
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('EXEC QUERY DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno, _sql_query)
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(_err))
            }
    async def run(self, _tables, _query):
        '''execute query'''
        try:
            user = self.params.get('user')
            # ROLE TABLE ACCESS
            permissions = self.params.get('permissions')
            if user.get('role_id') != 1:
                crud_aciton = 'read'
                for _table in _tables:
                    _chk = self.check_table_access(_table, permissions, crud_aciton)
                    if not _chk:
                        return {'success': False, 'msg': self.i18n('access-problem')}
                    elif _chk.get('success') is False:
                        return _chk
            engine = self.db.get_engine()
            # print('QUERY ENGINE:', engine.url)
            inspector = self.db.get_inspector(engine)
            metadata = self.db.get_metadata(engine)
            # CHECK ROW LEVEL ACCESS       
            _table = _tables[0]
            tbl = Table(_tables[0], metadata, autoload_with = engine)
            fields = list(map(lambda field: field.get('name'), inspector.get_columns(_table)))
            pk, fks = await self.db.get_constraint(metadata, inspector, _table)
            _tables_to_check_row_level = []
            _rls_fk_table_ref = {}
            _row_level_access = {}
            if user.get('role_id') != 1:
                if not self.params.get('row_level_tables'):
                    pass
                elif len(self.params['row_level_tables']) > 0:
                    if _table in self.params['row_level_tables']:
                        _tables_to_check_row_level.append(_table)
                    for ref_field in fks:
                        _tables_to_check_row_level.append(fks[ref_field].get('referred_table'))
                        _rls_fk_table_ref[fks[ref_field].get('referred_table')] = fks[ref_field]               
                if len(_tables_to_check_row_level) > 0:
                    _access = Access(self.conf, self.params, self.db, self.i18n)
                    _row_level_access = await _access.row_level_access(tables = _tables_to_check_row_level)
            msg = ''
            fields = []
            _rls_conds = []
            _extra_conds = {}
            if user.get('role_id') != 1:
                if len(_tables_to_check_row_level) > 0:
                    # CHECK IF THE CURRENT TABLE IS HAS RLS
                    for _rls_table in _tables_to_check_row_level:
                        _row_ids = []
                        if _row_level_access.get(_rls_table):
                            filtered_read_access = filter (
                                lambda rls: rls.get('read') is True,
                                _row_level_access.get(_rls_table)
                            )
                            _row_ids = list(
                                map(
                                    lambda rls: rls.get('row_id'), # RETURNS ONLY THE ID's
                                    filtered_read_access
                                )
                            )
                        print('RLS:', _rls_table, _row_ids)
                        if len(_row_ids) == 0 and _rls_table == _table:
                            msg = self.i18n('row-level-access', table = _rls_table, action = 'read')
                        if len(_row_ids) == 0 and _rls_table != _table:
                            if _rls_table in ['user']:
                                continue
                            msg = self.i18n('fk-row-level-access', table_org = _table, table = _rls_table, action = 'read')
                        if _rls_table == _table:
                            #_rls_conds.append(tbl.c[pk].in_(copy.deepcopy(_row_ids)))                            
                            _rls_conds.append(f""""{_table}"."{pk}" IN ({','.join(_row_ids)})""")
                            _extra_conds[_table] = f""""{_table}"."{pk}" IN ({','.join(_row_ids)})"""
                        else:
                            fk = _rls_fk_table_ref[_rls_table].get('constrained_column')
                            _fk = _rls_fk_table_ref[_rls_table].get('referred_column')
                            _rls_conds.append(f""""{_rls_table}"."{_fk}" IN ({','.join(_row_ids)})""")
                            _extra_conds[_table] = f""""{_rls_table}"."{_fk}" IN ({','.join(_row_ids)})"""
            sql = self.get_query(_query, _extra_conds, fields, user)
            if self.params['data'].get('filters'):
                sql = self.set_filters(sql, filters = self.params['data'].get('filters'))
            # print(sql)
            return await self.exec_query(engine, sql)
        except Exception as _err:# pylint: disable=broad-exception-caught
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno, _query)
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(_err))
            }
