
#https://docs.sqlalchemy.org/en/20/tutorial/data_update.html
'''Create and Update Logic'''
import json
import os
import sys
import datetime
import re
import copy
from dateutil import parser
from sqlalchemy import Table
from sqlalchemy.sql import select, and_, insert, update, delete
from passlib.context import CryptContext

from core.access import Access

pwd_context = CryptContext(schemes = ["bcrypt"], deprecated = "auto")

class CreateUpdate:
    '''Create and Update Logic'''
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
    # CREATE
    def re_fn(self, expr, item):
        '''regex'''
        reg = re.compile(expr, re.I)
        return reg.search(item) is not None
    async def run(self, _table, _data):
        '''execute Create and Update Logic'''
        try:
            user = self.params.get('user')
            engine = self.db.get_engine()
            inspector = self.db.get_inspector(engine)
            metadata = self.db.get_metadata(engine)
            tbl = Table(_table, metadata, autoload_with = engine)
            fields = inspector.get_columns(_table)
            _fields_names = list(map(lambda field: field.get('name'), fields))
            #pk = pk_constraint['constrained_columns'][0]
            data = copy.deepcopy(_data)
            pk, fks = await self.db.get_constraint(metadata, inspector, _table) # GET THE CONSTRAINTS
            # ROLE TABLE ACCESS
            permissions = self.params.get('permissions')
            # print(permissions)
            crud_aciton = 'create'
            if data.get(pk):
                if data.get('_to_delete') is True: #DELETE
                    crud_aciton = 'delete'
                elif data.get('excluded') is True: #DELETE
                    crud_aciton = 'delete'
                else: # UPDATE
                    crud_aciton = 'update'
            _chk = self.check_table_access(_table, permissions, crud_aciton)
            if not _chk:
                return {'success': False, 'msg': self.i18n('access-problem')}
            elif _chk.get('success') is False:
                return _chk
            # CHECK ROW LEVEL ACCESS
            if crud_aciton != 'create' and data.get(pk) and user.get('role_id') != 1:
                _tables_to_check_row_level = []
                _rls_fk_table_ref = {}
                #print(self.params.get('row_level_tables'), 'FKS:', fks)
                if not self.params.get('row_level_tables'):
                    pass
                elif len(self.params['row_level_tables']) > 0: # IN CASE THERE IS TABLES THAT REQUIRES ROW LEVEL ACCESS
                    if _table in self.params['row_level_tables']:
                        _tables_to_check_row_level.append(_table)
                    for ref_field in fks:
                        if fks[ref_field].get('referred_table') in self.params['row_level_tables']:
                            _tables_to_check_row_level.append(fks[ref_field].get('referred_table'))
                        _rls_fk_table_ref[fks[ref_field].get('referred_table')] = fks[ref_field]
                if len(_tables_to_check_row_level) > 0:
                    _access = Access(self.conf, self.params, self.db, self.i18n)
                    for _rls_table in _tables_to_check_row_level:
                        if _rls_table != _table:
                            if _rls_table in ['user']:
                                continue
                        row_id = data.get(pk)
                        if _rls_table != _table:
                            fk = _rls_fk_table_ref[_rls_table].get('constrained_column')
                            row_id = data.get(fk)
                        _row_level_access = await _access.row_level_access(tables = _rls_table, row_id = row_id)
                        if not _row_level_access.get(_rls_table):
                            _row_level_access = {}
                        elif len(_row_level_access.get(_rls_table)) > 0:
                            _row_level_access = _row_level_access[_rls_table][0]
                        else:
                            _row_level_access = {}
                        print('RLS:', _rls_table, crud_aciton, row_id, _row_level_access)
                        msg = ''
                        if _rls_table == _table and not _row_level_access.get(crud_aciton):
                            msg = self.i18n('row-level-access-row-id', table = _rls_table, action = crud_aciton, row_id = str(row_id))
                        elif _rls_table != _table and not _row_level_access.get(crud_aciton):
                            msg = self.i18n('fk-row-level-access-row-id', table_org = _table, table = _rls_table, action = crud_aciton, row_id = str(row_id))
                        if 'user_id' not in _fields_names and msg != '':
                            return {'success': False, 'msg': msg}
                        elif msg != '':
                            query = select([tbl.c.user_id])\
                                    .select_from(tbl)\
                                    .where(and_(
                                        tbl.c.user_id == user.get('user_id'),
                                        tbl.c[pk] == row_id
                                    ))
                            #print('IT HAS USER ID, CHECK IF IS THE SAME USER:', str(query))
                            with engine.connect() as conn:
                                result = conn.execute(query)
                                res = result.mappings().all()
                                result.close()
                                conn.close()
                                if len(res) > 0:
                                    pass
                                else:
                                    return {'success': False, 'msg': msg}
            errors = []
            #PROCESS FIELDS
            for field in fields:
                datatype = None
                try:
                    datatype = str(field['type'])
                    if datatype.find('(') != -1:
                        datatype = datatype[0:datatype.find('(')].lower()
                    else:
                        datatype = datatype.lower()
                    #print(field['name'], datatype)
                    # CONVERT DATES / DATETIMES
                    if datatype in ['datetime'] and data.get(field['name']) and not isinstance(data.get(field['name']), datetime.datetime):
                        # print(field['name'], self.params['data'].get('timezone'), data[field['name']], parser.parse(data[field['name']]))
                        data[field['name']] = parser.parse(data[field['name']])
                    elif datatype in ['date'] and data.get(field['name']) and not isinstance(data.get(field['name']), datetime.date):
                        data[field['name']] = parser.parse(data[field['name']]).date()
                    elif datatype in ['time'] and data.get(field['name']) and not isinstance(data.get(field['name']), datetime.time):
                        print(field['name'], self.params['data'].get('timezone'), data[field['name']], parser.parse(data[field['name']]))
                        try:
                            aux = datetime.datetime.strptime(data[field['name']], '%H:%M')
                        except Exception as _err:# pylint: disable=broad-exception-caught
                            aux = datetime.datetime.strptime(data[field['name']], '%H:%M:%S')
                        data[field['name']] = aux.time()
                        print(data[field['name']])
                    # VALIDATIONS
                    if field['name'] in ['created_at', 'updated_at']:
                        if data.get(pk) and field['name'] in ['created_at'] and data.get(field['name']): # IN CASE THE PK EXISTS THE RECORD ALREAD EXIST SO DO CREATED_AT
                            pass
                            #print('ALREADY CREATED:', field['name'], data.get(field['name']))
                        else:
                            data[field['name']] = datetime.datetime.now()
                    elif field['name'] in ['excluded']:
                        if not data.get(pk):
                            data[field['name']] = False
                    elif field['name'] in ['password', 'pass']:
                        if not data.get(pk):
                            data[field['name']] = pwd_context.hash(data[field['name']])
                        elif len(data[field['name']]) < 20:
                            #print('password:', data[field['name']], len(data[field['name']]))
                            data[field['name']] = pwd_context.hash(data[field['name']])
                    elif field['name'] in ['app', 'app_id'] and _table not in ['app', 'role_app', 'role_app_menu', 'role_app_menu_table']:
                        if not data.get(pk) and not data.get(field['name']):
                            data[field['name']] = self.params['app'].get(field['name'])
                    elif field['name'] in ['user', 'user_id'] and _table not in ['user']:
                        if not data.get(pk) and not data.get(field['name']):
                            data[field['name']] = self.params['user'].get(field['name'])
                    elif type(data.get(field['name'])) in [dict, list]:
                        data[field['name']] = json.dumps(data[field['name']])
                    elif field['nullable'] is False and not data.get(field['name']) and field['name'] != pk and not data.get('_to_delete'):
                        if field['name'] == 'lang':
                            data[field['name']] = self.params.get(field['name'])
                        elif field['name'] == 'db':
                            data[field['name']] = self.params['app'].get(field['name'])
                        else:
                            errors.append(self.i18n('field-required', field = field['name']))
                except Exception as _err:# pylint: disable=broad-exception-caught
                    print(str(_err))
            if len(errors) > 0:
                return {
                    'success': False,
                    'msg': self.i18n('validation-errors', n = str(len(errors))),
                    'errors': errors
                }
            # REMOVE FIELDS THAT IS NOT IN THE DB
            _org_data = copy.deepcopy(data)
            for field in _org_data:
                if not field in _fields_names:
                    del data[field]
            if data.get(pk): #UPDATE
                pk_val = copy.deepcopy(data[pk])
                del data[pk]
                if _org_data.get('_to_delete') is True: #DELETE
                    if 'excluded' in _fields_names and not _org_data.get('permanently'): # SETS EXCLUDED 2 TRUE
                        sql = update(tbl)\
                                .where(tbl.c[pk] == pk_val)\
                                .values({'excluded': True})
                    else: # PERMANENTLY DELETE
                        sql = delete(tbl)\
                                .where(tbl.c[pk] == pk_val)
                else:
                    sql = update(tbl)\
                            .where(tbl.c[pk] == pk_val)\
                            .values(data)
            else: #CREATE
                sql = insert(tbl).values(data)
            #print(str(sql), data)
            with engine.connect() as conn:
                # print('drivername:', engine.driver)
                if engine.driver in ['sqlite', 'pysqlite']:
                    #conn.execute('BEGIN CONCURRENT')
                    pass
                result = conn.execute(sql)
                ins_id = None
                try:
                    ins_id  = result.inserted_primary_key[0]
                    _data[pk] = ins_id
                except Exception as _err:# pylint: disable=broad-exception-caught
                    pass
                result.close()
                try:
                    conn.commit()
                except Exception as _err:# pylint: disable=broad-exception-caught
                    *_, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
                conn.close()
                return {
                    'success': True,
                    'msg': self.i18n('success'),
                    'pk': pk,
                    'inserted_primary_key': ins_id,
                    'data': _data if ins_id else None,
                    'sql': str(sql)
                }
        except Exception as _err:# pylint: disable=broad-exception-caught
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(_err))
            }
