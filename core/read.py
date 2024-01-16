'''The R(read) in cRud'''
# pylint: disable=unused-variable
# pylint: disable=line-too-long
# pylint: disable=wrong-import-order
# pylint: disable=broad-exception-caught
# pylint: disable=unused-import
# pylint: disable=invalid-name
# pylint: disable=trailing-whitespace
# pylint: disable=not-callable
import os
import sys
import datetime
import re
import copy
from sqlalchemy import Table
from sqlalchemy.sql import select
from sqlalchemy.sql import or_
from sqlalchemy.sql import and_
from sqlalchemy.sql import desc
from sqlalchemy.sql import text
from sqlalchemy import func, alias
from core.access import Access
class Read:
    '''The R(read) in cRud'''
    def __init__(self, conf, params, db, i18n):
        self.conf = conf
        self.params = params
        self.db = db
        self.i18n = i18n
    #CHECK TABLE PERMISSIONS
    def check_table_access(self, table, permissions, crud_action):
        '''check current user table access'''
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
    # READ
    def re_fn(self, expr, item):
        '''sqlite regex fn'''
        reg = re.compile(expr, re.I)
        return reg.search(item) is not None
    async def run(self, _table):
        '''Executes the reading'''
        try:
            user = self.params.get('user')
            # ROLE TABLE ACCESS
            permissions = self.params.get('permissions')
            if user.get('role_id') != 1:
                # print(permissions)
                crud_aciton = 'read'
                _chk = self.check_table_access(_table, permissions, crud_aciton)
                if not _chk:
                    return {'success': False, 'msg': self.i18n('access-problem')}
                elif _chk.get('success') is False:
                    return _chk
            engine = self.db.get_engine()
            #print(engine.url, self.params.get('app'))
            inspector = self.db.get_inspector(engine)
            metadata = self.db.get_metadata(engine)
            tbl = Table(_table, metadata, autoload_with = engine)
            pk, fks = await self.db.get_constraint(metadata, inspector, _table) # GET THE CONSTRAINTS
            # CHECK ROW LEVEL ACCESS
            _tables_to_check_row_level = []
            _rls_fk_table_ref = {}
            _row_level_access = {}
            if user.get('role_id') != 1:
                if not self.params.get('row_level_tables'):
                    pass
                elif len(self.params['row_level_tables']) > 0: # IN CASE THERE IS TABLES THAT REQUIRES ROW LEVEL ACCESS
                    if _table in self.params['row_level_tables']:
                        _tables_to_check_row_level.append(_table)
                    for ref_field in fks:
                        if fks[ref_field].get('referred_table') in self.params['row_level_tables']:
                            _tables_to_check_row_level.append(fks[ref_field].get('referred_table'))
                        _rls_fk_table_ref[fks[ref_field].get('referred_table')] = fks[ref_field]
                # print(self.params['row_level_tables'], '_tables_to_check_row_level:', _tables_to_check_row_level)          
                if len(_tables_to_check_row_level) > 0:
                    _access = Access(self.conf, self.params, self.db, self.i18n)
                    _row_level_access = await _access.row_level_access(tables = _tables_to_check_row_level)
                print('_row_level_access:', _row_level_access)
            fields = list(map(lambda field: field.get('name'), inspector.get_columns(_table)))
            flds = [tbl]
            _joins = tbl
            pk_constraint = inspector.get_pk_constraint(_table)
            pk = pk_constraint['constrained_columns'][0]
            _join_overwrite = {}
            if self.params['data'].get('join_overwrite'):
                _join_overwrite = self.params['data'].get('join_overwrite')
            #print('_join_overwrite:', _join_overwrite)
            if self.params['data'].get('join') == 'none' \
                and (not _join_overwrite.get(_table) or _join_overwrite.get(_table) == 'none'):
                pass
            else:
                #pk, fks = await self.db.get_constraint(metadata, inspector, _table)
                self_joins = {}
                for fk in fks:
                    if fks[fk].get('referred_table') == _table:
                        if not self_joins.get(_table):
                            self_joins[_table] = []
                        self_joins[_table].append(_table)
                        continue
                    fks[fk]['fk_table'] = Table(fks[fk].get('referred_table'), metadata, autoload_with = engine)
                    #print(fks[fk]['fk_table'], fks[fk]['fk_table'].c[fks[fk]['referred_column']], tbl.c[fks[fk]['constrained_column']] )
                    #fks[fk]['fields'] = list(map(lambda field: field.get('name'), inspector.get_columns(fk)))
                    fks[fk]['fields'] = list(map(lambda field: field.get('name'), inspector.get_columns(fks[fk].get("referred_table"))))
                    if self.params['data'].get('join') == 'all' or _join_overwrite.get(_table) == 'all':
                        _all_fields = list(
                            map(
                                lambda field: f'"{fks[fk].get("referred_table")}"."{field.get("name")}" AS "{fks[fk].get("referred_table")}_{field.get("name")}"', # pylint: disable = cell-var-from-loop
                                inspector.get_columns(fks[fk].get("referred_table"))
                            )
                        )
                        #_all_fields = list(filter( lambda field: field not in fields, _all_fields))
                        #print(_all_fields)
                        _sql_fields = ','.join(_all_fields)
                        #print(_sql_fields)
                        flds.append(text(f'{_sql_fields}'))
                    else:
                        # print('fk', fk, fks[fk]['referred_table'], fks[fk]['referred_columns_desc'], fks[fk]['referred_columns_desc'] in fields)
                        if not fks[fk]['referred_columns_desc'] in fields:
                            if fks[fk].get('referred_table') == _table:
                                flds.append(fks[fk]['fk_table'].c[fks[fk]['referred_columns_desc']]) #.alias()
                            else:
                                flds.append(fks[fk]['fk_table'].c[fks[fk]['referred_columns_desc']])
                    _joins = _joins.outerjoin(
                        fks[fk]['fk_table'],
                        fks[fk]['fk_table'].c[fks[fk]['referred_column']] == tbl.c[fks[fk]['constrained_column']]
                    ) # https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-joins
            flds_aux = []
            if self.params['data'].get('fields'):
                # print(self.params['data'].get('fields'), fields)
                try:
                    if isinstance(self.params['data'].get('fields'), dict):
                        if self.params['data']['fields'].get(_table):
                            # print(_table, self.params['data']['fields'].get(_table))
                            for field in self.params['data']['fields'].get(_table):
                                if field in fields:
                                    flds_aux.append(tbl.c[field])
                                else:
                                    for fk in fks:
                                        if fks[fk].get('fields') and fks[fk].get('fk_table'):
                                            if field in fks[fk]['fields']:
                                                flds_aux.append(fks[fk]['fk_table'].c[field])
                    elif isinstance(self.params['data'].get('fields'), list):
                        for field in self.params['data'].get('fields'):
                            if field in fields:
                                flds_aux.append(tbl.c[field])
                            else:
                                for fk in fks:
                                    if fks[fk].get('fields') and fks[fk].get('fk_table'):
                                        if field in fks[fk]['fields']:
                                            flds_aux.append(fks[fk]['fk_table'].c[field])
                    if len(flds_aux) > 0:
                        flds = flds_aux
                except Exception as _err:# pylint: disable=broad-exception-caught
                    *_, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            sql = select(*flds)\
                .select_from(_joins)
            if self.params['data'].get('distinct') is True:
                sql = select(*flds).distinct()\
                    .select_from(_joins)
            # CONDITIONS
            #print('DATA:', self.params['data'])
            if 'excluded' in fields:
                sql = sql.where(tbl.c['excluded'] == False)
            if self.params['data'].get('filters'):
                for fil in self.params['data'].get('filters'):                   
                    if self.params['data'].get('ignore_filter'): # IN CASE OF READ WITH MULTIPLE TABLE U CAN CHOOSE TO IGNORE TEM FOR SOME TABLE
                        # print(self.params['data'].get('ignore_filter'), _table, fil.get('field'))
                        if self.params['data']['ignore_filter'].get(_table) == fil.get('field'):
                            continue
                        elif isinstance(self.params['data']['ignore_filter'].get(_table), list):
                            if fil.get('field') in self.params['data']['ignore_filter'].get(_table):
                                continue
                        elif isinstance(self.params['data']['ignore_filter'].get(_table), dict):
                            if fil.get('field') in self.params['data']['ignore_filter'].get(_table):
                                continue
                    if self.params['data'].get('apply_only_to'): # IF SET "apply_only_to", ONLY THE TABLES SPECIFIED WILL BE FILTERED
                        if not self.params['data']['apply_only_to'].get(_table):
                            print('apply_only_to:', _table)
                            continue
                    if fil.get('field') in fields:
                        if not fil.get('cond') or fil.get('cond') == '=':
                            sql = sql.where(tbl.c[fil.get('field')] == fil.get('value'))
                        elif fil.get('cond').lower() in ['=', '!=', '>', '<', '>=', '<=']:
                            sql = sql.where(text(f"""{tbl.c[fil.get('field')]} {fil.get('cond')} '{fil.get('value')}'"""))
                        elif fil.get('cond').lower() in ['in', 'not in']:
                            val = fil.get('value') if fil.get('value') else ''
                            values = val.split(',') if len(val.split(',')) > 1 else val.split(';')
                            values = "', '".join(values)
                            sql = sql.where(text(f"""{tbl.c[fil.get('field')]} {fil.get('cond')} ('{values}')"""))
                        elif fil.get('cond').lower() in ['between', 'not between']:
                            val = fil.get('value') if fil.get('value') else ''
                            values = val.split(',') if len(val.split(',')) > 1 else val.split(';')
                            if len(values) < 2:
                                return {
                                    'success': False,
                                    'msg': f"Condition '{fil.get('cond')}' not valid value must be like A,B ou A;B!"
                                }
                            sql = sql.where(text(f"""{tbl.c[fil.get('field')]} {fil.get('cond')} '{values[0]}' AND '{values[1]}' """))
                        elif fil.get('cond').lower() in ['like', 'not like']:
                            val = f"%{fil.get('value')}%"
                            if fil.get('value').find('%') != -1:
                                val = fil.get('value')
                            sql = sql.where(text(f"""{tbl.c[fil.get('field')]} {fil.get('cond')} '{val}'"""))
                        elif fil.get('cond').lower() in ['is true', 'is false', 'is null', 'is not null']:
                            sql = sql.where(text(f"""{tbl.c[fil.get('field')]} {fil.get('cond')}"""))
            # TEXT SEARCH
            _split_pattern = re.compile(r'\||\;')
            _apply_patt_only = self.params['data'].get('apply_patt_only')
            #print(_apply_patt_only)
            if self.params['data'].get('pattern'):
                aux = []
                _skip = False
                if not _apply_patt_only:
                    pass
                elif not isinstance(_apply_patt_only, list):
                    pass
                elif _table not in _apply_patt_only:
                    _skip = True
                    #print('_apply_patt_only:', _table)
                #print(_skip, _table)
                if _skip is False:
                    key = f"%{self.params['data'].get('pattern')}%"
                    key2 = f"{self.params['data'].get('pattern')}"
                    _splited_keys = re.split(_split_pattern, key2)
                    #print(_table, key, key2, _splited_keys)
                    if self.params['data'].get('pattern').find('%') != -1:
                        key = f"{self.params['data'].get('pattern')}"    
                    for fil in fields:
                        if fil == pk:
                            continue
                        elif fks.get(fil):
                            continue
                        if len(_splited_keys) > 1:
                            for k in _splited_keys:
                                _key = k.strip()
                                if _key.find('%') == -1:
                                    _key = f'%{_key}%'
                                aux.append(tbl.c[fil].like(_key))
                        else:
                            aux.append(tbl.c[fil].like(key))
                            if engine.name in ['sqlite']:
                                pass
                                # aux.append(text(f'"{_table}"."{fil}" REGEXP \'{text(key2)}\''))
                    for fk in fks:
                        try:
                            if len(_splited_keys) > 1:
                                for k in _splited_keys:
                                    _key = k.strip()
                                    if _key.find('%') == -1:
                                        _key = f'%{_key}%'
                                aux.append(tbl.c[fil].like(_key))
                            else:                            
                                aux.append(fks[fk]['fk_table'].c[fks[fk]['referred_columns_desc']].like(key))
                        except Exception as __err:
                            pass   
                    sql = sql.where(or_(*aux))
            # ROW LEVEL ACCESS
            msg = ''
            if user.get('role_id') != 1:
                _rls_conds = []
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
                            #sql = sql.where(tbl.c[pk].in_(copy.deepcopy(_row_ids)))
                            _rls_conds.append(tbl.c[pk].in_(copy.deepcopy(_row_ids)))
                        else:
                            fk = _rls_fk_table_ref[_rls_table].get('constrained_column')
                            _fk = _rls_fk_table_ref[_rls_table].get('referred_column')
                            if type(fks[fk].get('fk_table')) != "<class 'sqlalchemy.sql.schema.Table'>": # IN CASE THE TABLE IS NOT REFLECTED YET
                                fks[fk]['fk_table'] = Table(_rls_table, metadata, autoload_with = engine)
                            #sql = sql.where(fks[fk]['fk_table'].c[fk].in_(copy.deepcopy(_row_ids)))
                            _rls_conds.append(fks[fk]['fk_table'].c[_fk].in_(copy.deepcopy(_row_ids)))
                    if 'user_id' not in fields:
                        sql = sql.where(and_(*_rls_conds))
                    else:
                        sql = sql.where(
                            or_(
                                and_(and_(*_rls_conds)),
                                tbl.c.user_id == user.get('user_id')
                            )
                        )
            # ORDER BY
            if self.params['data'].get('order_by'):
                for fil in self.params['data'].get('order_by'):
                    if fil.get('field') in fields:
                        if fil['order'] == 'DESC':
                            sql = sql.order_by(desc(tbl.c[fil.get('field')]))
                        else:
                            sql = sql.order_by(tbl.c[fil.get('field')])
            # SQL TO GET TOTAL ROWS
            # print(sql, flds)
            cnt_field = fields[0]
            if len(flds_aux) > 0:
                cnt_field = flds_aux[0].name
            sql_total = select(func.count(sql.c[cnt_field]))
            #print(str(sql_total))
            # LIMIT & OFFSET
            limit = self.params['data'].get('limit') if self.params['data'].get('limit') else 10
            offset = self.params['data'].get('offset') if self.params['data'].get('offset') else 0
            #print(f'LIMIT {str(limit)} OFFSET {str(offset)}')
            if limit == -1:
                pass
            else:
                sql = sql.limit(limit).offset(offset)
            # print(2, sql)#, sql_total)
            # EXECUTE
            data = []
            total = 0
            with engine.connect() as conn:
                #conn.connection.create_function('regexp', 2, self.re_fn)
                result = conn.execute(sql)
                res = result.mappings().all()
                result.close()
                for row in res:
                    d = {}
                    for column in row:
                        d[column] = row[column]
                        if isinstance(d[column], (datetime.datetime, datetime.date)):
                            d[column] = d[column].isoformat()
                        # CHECK ROW LEVEL ACCESS
                    data.append(d)
                result = conn.execute(sql_total)
                res = result.mappings().all()
                result.close()
                total = res[0]['count_1']
                # print('total:', total)
            return {
                'success': True,
                'msg': msg if msg else self.i18n('success'),
                'data': data,
                'total': total,
                'permissions': permissions.get(_table) if permissions else {},
                'sql': str(sql)
            }
        except Exception as _e:
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_e), fname, exc_tb.tb_lineno)
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(_e))
            }
