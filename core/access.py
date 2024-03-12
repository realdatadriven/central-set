# pylint: disable=unused-variable
# pylint: disable=line-too-long
# pylint: disable=wrong-import-order
# pylint: disable=broad-exception-caught
# pylint: disable=unused-import
# pylint: disable=invalid-name
# pylint: disable=trailing-whitespace
# pylint: disable=not-callable
# pylint: disable=singleton-comparison
# pylint: disable=broad-exception-caught
# pylint: disable=bare-except
'''Manage the access to table and rows'''
import os
import sys
import datetime
import copy
import re
from sqlalchemy import Table
from sqlalchemy.sql import select
from sqlalchemy.sql import join
from sqlalchemy.sql import and_
from sqlalchemy.sql import union

from core.db import DB

class Access:
    '''Manage the access to table and rows'''
    def __init__(self, conf, params, db: DB, i18n):
        self.conf = conf
        self.params = params
        self.db = db
        self.i18n = i18n
    async def commons(self):
        '''auxiliar that returns commons parameters'''
        try:
            # DATABASE CONNECTION
            admin_db_conf = copy.deepcopy(self.conf.get('DATABASE'))
            engine = self.db.get_engine()
            admin_db = None
            #print(engine.url.database, admin_db_conf.get('database'))
            if str(engine.url.database).find(str(admin_db_conf.get('database'))) == -1:
                #print(2, engine.url.database, admin_db_conf.get('database'))
                admin_db = DB(self.conf, {})
            aux_new_db = ''
            if self.params.get('db'):
                if isinstance(self.params.get('db'), dict):
                    aux_new_db = self.params['db'].get('database')
                elif isinstance(self.params.get('db'), str):
                    aux_new_db = self.params.get('db')
            if self.params.get('database'):
                if isinstance(self.params.get('database'), dict):
                    aux_new_db = self.params['database'].get('database')
                else:
                    aux_new_db = self.params.get('database')
            elif self.params.get('db'):
                if isinstance(self.params.get('db'), dict):
                    aux_new_db = self.params['db'].get('database')
                else:
                    aux_new_db = self.params.get('db')
            elif self.params.get('app'):
                if self.params['app'].get('db'):
                    if isinstance(self.params['app'].get('db'), dict):
                        aux_new_db = self.params['app']['db'].get('database')
                    else:
                        aux_new_db = self.params['app'].get('db')
                elif self.params['app'].get('database'):
                    if isinstance(self.params['app'].get('database'), dict):
                        aux_new_db = self.params['app']['database'].get('database')
                    else:
                        aux_new_db = self.params['app'].get('database')
            if str(engine.url.database).find(str(aux_new_db)) == -1:
                new_db = DB(self.conf, self.params)
                engine = new_db.get_engine()
            inspector = self.db.get_inspector(engine)
            metadata = self.db.get_metadata(engine)
            metadata.reflect(engine)
            # USER / APP / DB
            user = self.params.get('user')
            app = self.params.get('app')
            if not app:
                app = self.params['data'].get('app')
            if not app:
                app = {}
            app_id = app.get('app_id')
            if not app_id:
                app_id = 1
            database = str(engine.url.database)
            if engine.url.drivername == 'sqlite':
                database = re.sub(re.compile(r'.+\/'), '', database)
                database = re.sub(re.compile(r'\.db'), '', database)
            if app.get('db'):
                database = app.get('db')
            lang = 'en'
            if self.params.get('lang'):
                lang = self.params.get('lang')
            #print('get tables lang:', lang)
            return (engine, metadata, inspector, app, app_id, database, user, lang, admin_db)
        except Exception as _err:# pylint: disable=broad-exception-caught
            try:
                engine.dispose()
            except Exception as _err2:# pylint: disable=broad-exception-caught
                pass
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(_err))
            }
    async def table_access(self, tables = None):
        '''table access'''
        try:
            engine, metadata, inspector, app, app_id, database, user, lang, admin_db = await self.commons()
            #print(engine, metadata, inspector, app, app_id, database, user)
            #print('admin_db:', admin_db)
            if not tables:
                if not self.params.get('data'):
                    pass
                elif self.params['data'].get('tables'):
                    if isinstance(self.params['data'].get('tables'), list):
                        tables = copy.deepcopy(self.params['data'].get('tables'))
                    else:
                        tables = [self.params['data'].get('tables')]
                elif self.params['data'].get('table'):
                    if isinstance(self.params['data'].get('table'), list):
                        tables = copy.deepcopy(self.params['data'].get('table'))
                    else:
                        tables = [self.params['data'].get('table')]
            elif isinstance(tables, str):
                tables = [tables]
            elif isinstance(tables, dict) and len(tables) > 0:
                pass
            if not tables or len(tables) == 0:
                tables = inspector.get_table_names()
            with engine.connect() as conn:
                try:
                    _admin_engine = engine
                    _admin_metadata = metadata
                    if admin_db:
                        _admin_engine = admin_db.get_engine()
                        _admin_metadata = admin_db.get_metadata(_admin_engine)
                    else:
                        admin_db = self.db
                    _admin_conn = _admin_engine.connect()
                    # USER ROLES
                    roles = [user.get('role_id')]
                    # print('table_access:', tables, 'roles:', roles)
                    tbl = Table('user_role', _admin_metadata, autoload_with = _admin_engine)
                    join_tbl = Table('role', _admin_metadata, autoload_with = _admin_engine)
                    _join = join(tbl, join_tbl, tbl.c.role_id == join_tbl.c.role_id)
                    query = select(tbl.c).\
                        select_from(_join).\
                        where(
                            and_(
                                tbl.c.user_id == user.get('user_id'),
                                tbl.c.excluded == False,
                                join_tbl.c.excluded == False
                            )
                        )
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                        roles.append(d['role_id'])
                    #print('ROLLES:', roles)
                    tbl = Table('role_app_menu_table', _admin_metadata, autoload_with = _admin_engine)
                    join_tbl = Table('table', _admin_metadata, autoload_with = _admin_engine)
                    _join = join(tbl, join_tbl, tbl.c.table_id == join_tbl.c.table_id)
                    query = select(tbl.c, join_tbl.c.table).\
                        select_from(_join).\
                        where(
                            and_(
                                join_tbl.c.table.in_(tables),
                                tbl.c.role_id.in_(roles),
                                tbl.c.app_id == app_id,
                                tbl.c.excluded == False,
                                join_tbl.c.excluded == False
                            )
                        )
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    #print(str(query), {'app_id': app_id, 'role_id': roles, 'tables': tables}, data)
                    table_data = {}
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        table_data[d['table']] = d
                    result.close()
                    conn.close()
                    engine.dispose()
                    try:
                        _admin_conn.close()
                        _admin_engine.dispose()
                    except:
                        pass
                    return table_data
                except Exception as e:
                    try:
                        conn.close()
                        engine.dispose()
                    except:
                        pass
                    *_, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print('DEBUG INF: ', str(e), fname, exc_tb.tb_lineno)
                    return {
                        'success': False,
                        'msg': self.i18n('unexpected-error', err = str(e))
                    }
        except Exception as e:
            try:
                engine.dispose()
            except:
                pass
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(e), fname, exc_tb.tb_lineno)
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(e))
            }
    async def permissions(self, tables = None):
        '''permissions'''
        return await self.table_access(tables)
    async def row_level_access(self, tables = None, row_id = None):
        '''row level access'''
        try:
            engine, metadata, inspector, app, app_id, database, user, lang, admin_db = await self.commons()
            #print(engine, metadata, inspector, app, app_id, database, user)
            #print('admin_db:', admin_db)
            if not tables:
                if not self.params.get('data'):
                    pass
                elif self.params['data'].get('tables'):
                    if isinstance(self.params['data'].get('tables'), list):
                        tables = copy.deepcopy(self.params['data'].get('tables'))
                    else:
                        tables = [self.params['data'].get('tables')]
                elif self.params['data'].get('table'):
                    if isinstance(self.params['data'].get('table'), list):
                        tables = copy.deepcopy(self.params['data'].get('table'))
                    else:
                        tables = [self.params['data'].get('table')]
            elif isinstance(tables, str):
                tables = [tables]
            elif isinstance(tables, dict) and len(tables) > 0:
                pass
            if not tables or len(tables) == 0:
                tables = []#inspector.get_table_names()
            with engine.connect() as conn:
                try:
                    _admin_engine = engine
                    _admin_metadata = metadata
                    if admin_db:
                        _admin_engine = admin_db.get_engine()
                        _admin_metadata = admin_db.get_metadata(_admin_engine)
                    else:
                        admin_db = self.db
                    _admin_conn = _admin_engine.connect()
                    # USER ROLES
                    roles = [user.get('role_id')]                    
                    # print('table_access:', tables, 'roles:', roles)
                    tbl = Table('user_role', _admin_metadata, autoload_with = _admin_engine)
                    join_tbl = Table('role', _admin_metadata, autoload_with = _admin_engine)
                    _join = join(tbl, join_tbl, tbl.c.role_id == join_tbl.c.role_id)
                    query = select(tbl.c).\
                        select_from(_join).\
                        where(
                            and_(
                                tbl.c.user_id == user.get('user_id'),
                                tbl.c.excluded == False,
                                join_tbl.c.excluded == False
                            )
                        )
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                        roles.append(d['role_id'])
                    #print('ROLLES:', roles)
                    tbl = Table('role_row_level_access', _admin_metadata, autoload_with = _admin_engine)
                    ___ = '''join_tbl = Table('table', _admin_metadata, autoload_with = _admin_engine)
                    _join = join(tbl, join_tbl, tbl.c.table_id == join_tbl.c.table_id)
                    query = select(tbl.c, join_tbl.c.table).\
                        select_from(_join).\
                        where(
                            and_(
                                join_tbl.c.table.in_(tables),
                                tbl.c.role_id.in_(roles),
                                tbl.c.app_id == app_id,
                                tbl.c.excluded == False,
                                join_tbl.c.excluded == False
                            )
                        )'''
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(
                            and_(
                                tbl.c.table.in_(tables),
                                tbl.c.role_id.in_(roles),
                                tbl.c.app_id == app_id,
                                tbl.c.excluded == False
                            )
                        )
                    _row_id = []
                    if not row_id:
                        pass
                    elif isinstance(row_id, list):
                        _row_id = row_id
                    else:
                        _row_id = [ row_id ]
                    if len(_row_id) == 0:
                        if not self.params['data'].get('row_id'):
                            pass
                        elif isinstance(self.params['data'].get('row_id'), list):
                            _row_id = self.params['data'].get('row_id')
                        else:
                            _row_id = [ self.params['data'].get('row_id') ]
                    if len(_row_id) > 0:
                        query = select(tbl.c).\
                            select_from(tbl).\
                            where(
                                and_(
                                    tbl.c.row_id.in_(_row_id),
                                    tbl.c.table.in_(tables),
                                    tbl.c.role_id.in_(roles),
                                    tbl.c.app_id == app_id,
                                    tbl.c.excluded == False
                                )
                            )
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    table_data = {}
                    for row in data:
                        d = {}
                        if not table_data.get(row['table']):
                            table_data[row['table']] = []
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        table_data[d['table']].append(d)
                    result.close()
                    conn.close()
                    engine.dispose()
                    try:
                        _admin_conn.close()
                        _admin_engine.dispose()
                    except:
                        pass
                    return table_data
                except Exception as e:
                    try:
                        conn.close()
                        engine.dispose()
                    except:
                        pass
                    *_, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print('DEBUG INF: ', str(e), fname, exc_tb.tb_lineno)
                    return {
                        'success': False,
                        'msg': self.i18n('unexpected-error', err = str(e))
                    }
        except Exception as e:
            try:
                engine.dispose()
            except:
                pass
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(e), fname, exc_tb.tb_lineno)
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(e))
            }
    async def row_level(self, tables = None):
        '''row level'''
        return await self.row_level_access(tables)
    async def row_level_tables(self):
        '''row level tables'''
        try:
            engine, metadata, inspector, app, app_id, database, user, lang, admin_db = await self.commons()
            with engine.connect() as conn:
                try:
                    _admin_engine = engine
                    _admin_metadata = metadata
                    if admin_db:
                        _admin_engine = admin_db.get_engine()
                        _admin_metadata = admin_db.get_metadata(_admin_engine)
                    else:
                        admin_db = self.db
                    _admin_conn = _admin_engine.connect()
                    tbl = Table('menu_table', _admin_metadata, autoload_with = _admin_engine)
                    join_tbl = Table('table', _admin_metadata, autoload_with = _admin_engine)
                    _join = join(tbl, join_tbl, tbl.c.table_id == join_tbl.c.table_id)
                    query = union(
                        select(join_tbl.c.table).\
                        select_from(_join).\
                        where(
                            and_(
                                tbl.c.app_id == app_id,
                                tbl.c.requires_rla == True,
                                tbl.c.excluded == False,
                                join_tbl.c.excluded == False
                            )
                        ),
                        select(join_tbl.c.table).\
                        select_from(join_tbl).\
                        where(
                            and_(
                                join_tbl.c.requires_rla == True,
                                join_tbl.c.excluded == False
                            )
                        )
                    )
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    tables = []
                    for row in data:
                        tables.append(row['table'])
                    try:
                        _admin_conn.close()
                        _admin_engine.dispose()
                    except:
                        pass
                    #print('tables:', tables)
                    return {'success': True, 'tables': tables}
                except Exception as e:
                    try:
                        conn.close()
                        engine.dispose()
                    except:
                        pass
                    *_, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print('DEBUG INF: ', str(e), fname, exc_tb.tb_lineno)
                    return {
                        'success': False,
                        'msg': self.i18n('unexpected-error', err = str(e))
                    }
        except Exception as e:
            try:
                engine.dispose()
            except:
                pass
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(e), fname, exc_tb.tb_lineno)
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(e))
            }
