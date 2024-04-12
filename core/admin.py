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
'''Admin modele'''
from core.db import DB
from core.access import Access
from core.crud import Crud
import os
import sys
import datetime
import copy
import re
from sqlalchemy import Table
from sqlalchemy.sql import select
from sqlalchemy.sql import join
from sqlalchemy.sql import and_
from sqlalchemy.sql import or_
from sqlalchemy.sql import union
from sqlalchemy.sql import distinct
from sqlalchemy.sql import desc
from sqlalchemy.dialects.mysql import base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Time,
    DateTime,
    Date,
    Float,
    Boolean,
    ForeignKey,
    UniqueConstraint
)
base.ischema_names['tinyint'] = base.BOOLEAN
base.ischema_names['mediumtext'] = base.TEXT


class Admin:
    '''Admin modele'''
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
    async def get_tables(self, tables = None):
        '''return tables'''
        try:
            # GET COMMONS
            engine, metadata, inspector, app, app_id, database, user, lang, admin_db = await self.commons()
            # GET THE TABLES
            #print('passed tables:', tables)
            if not tables:
                if not self.params.get('data'):
                    pass
                elif self.params['data'].get('tables'):
                    if isinstance(self.params['data'].get('tables'), list):
                        tables = copy.deepcopy(self.params['data'].get('tables'))
                    else:
                        tables = [self.params['data'].get('tables')]
                    #print(1, self.params['data'].get('tables'))
                elif self.params['data'].get('table'):
                    if isinstance(self.params['data'].get('table'), list):
                        tables = copy.deepcopy(self.params['data'].get('table'))
                    else:
                        tables = [self.params['data'].get('table')]
                    #print(2, self.params['data'].get('table'))
            elif isinstance(tables, str):
                tables = [tables]
            elif isinstance(tables, dict) and len(tables) > 0:
                pass
            if not tables or len(tables) == 0:
                #print(3, inspector.get_table_names())
                tables = inspector.get_table_names()
            _all_tables = inspector.get_table_names()
            # for views https://docs.sqlalchemy.org/en/20/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_view_names
            #print(tables, self.params['data'].get('table'))
            # print(engine.url.database)
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
                    # TABLE
                    tbl = Table('table', _admin_metadata, autoload_with = _admin_engine)
                    # CHECK IF ALL TABLE ARE REGISTRADED
                    query = select(tbl.c.table).\
                        select_from(tbl).\
                        where(and_(tbl.c.db == database, tbl.c.excluded == False))
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    _all_tables_reg = []
                    for row in data:
                        _all_tables_reg.append(row['table'])
                    result.close()
                    _tables_not_in = []
                    for _t in _all_tables:
                        if _t in _all_tables_reg or _t == 'sqlite_sequence':
                            continue
                        _tables_not_in.append(_t)
                    if len(_tables_not_in) > 0:
                        _data = []
                        for _t in _tables_not_in:
                            _data.append({
                                'table': _t, 
                                'db': database, 
                                'app_id': app_id,
                                '_table': tbl.name
                            })
                        _aux_params = copy.deepcopy(self.params)
                        _aux_params['data'] = {'table': tbl.name, 'data': _data}
                        _crud = Crud(self.conf, _aux_params, admin_db, self.i18n)
                        _upsert = await _crud.create_update()
                        # print('_tables_not_in:', _tables_not_in, _upsert)
                    # GET TABLE DATA
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(and_(
                            tbl.c.table.in_(tables),
                            tbl.c.db == database,
                            tbl.c.excluded == False
                        ))
                    # print(tables, database)
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    table_data = {}
                    table_data['_table'] = {}
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        table_data['_table'][d['table']] = d
                    result.close()
                    #print(tables, database, len(table_data['_table']))
                    #print(table_data['_table'])
                    # TABLE TRANSLATION
                    tbl = Table('translate_table', _admin_metadata, autoload_with = _admin_engine)
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(and_(
                            tbl.c.table.in_(tables),
                            tbl.c.db == database,
                            tbl.c.lang == lang,
                            tbl.c.excluded == False
                        ))
                    if lang != self.conf.get('FALLBACK_LANG') and self.conf.get('FALLBACK_LANG'):
                        query = union(query,
                            select(tbl.c).\
                            select_from(tbl).\
                            where(and_(
                                tbl.c.table.in_(tables),
                                tbl.c.db == database,
                                tbl.c.lang == self.conf.get('FALLBACK_LANG'),
                                tbl.c.excluded == False
                            ))
                        )
                    #print(str(query))
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    _tables_with_transl = []
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        if not table_data.get(d['table']):  
                            table_data[d['table']] = {}
                        # print(tbl.name, d['table'], lang)
                        _tables_with_transl.append(d['table'])
                        if lang != d.get('lang') and table_data[d['table']].get(tbl.name):
                            table_data[d['table']][tbl.name][d.get('lang')] = d
                        else:
                            table_data[d['table']][tbl.name] = d
                    result.close()
                    # FIELDS TRANSLATION
                    tbl = Table('translate_table_field', _admin_metadata, autoload_with = _admin_engine)
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(and_(
                            tbl.c.table.in_(tables),
                            tbl.c.db == database,
                            tbl.c.lang == lang,
                            tbl.c.excluded == False
                        ))
                    if lang != self.conf.get('FALLBACK_LANG') and self.conf.get('FALLBACK_LANG'):
                        query = union(query,
                            select(tbl.c).\
                            select_from(tbl).\
                            where(and_(
                                tbl.c.table.in_(tables),
                                tbl.c.db == database,
                                tbl.c.lang == self.conf.get('FALLBACK_LANG'),
                                tbl.c.excluded == False
                            ))
                        )
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        if not table_data.get(d['table']):  
                            table_data[d['table']] = {}
                        if not table_data[d['table']].get(tbl.name):
                            table_data[d['table']][tbl.name] = {}
                        if lang != d.get('lang') and table_data[d['table']][tbl.name].get(d['field']):
                            table_data[d['table']][tbl.name][d['field']][d.get('lang')] = d
                        else:
                            table_data[d['table']][tbl.name][d['field']] = d
                    result.close()
                    # TABLE CUSTOMIZATION
                    tbl = Table('custom_table', _admin_metadata, autoload_with = _admin_engine)
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(
                            or_(
                                (and_(
                                    tbl.c.table.in_(tables),
                                    tbl.c.db == database,
                                    tbl.c.user_id == user.get('user_id'),
                                    tbl.c.app_id == app_id,
                                    tbl.c.excluded == False
                                )),
                                (and_(
                                    tbl.c.table.in_(tables),
                                    tbl.c.db == database,
                                    tbl.c.user_id == 1,
                                    tbl.c.app_id == app_id,
                                    tbl.c.excluded == False
                                ))
                            )
                        ).\
                        order_by(tbl.c.custom_table_id.desc(), tbl.c.user_id.desc())#.\
                        #limit(1)
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        if not table_data.get(d['table']):
                            table_data[d['table']] = {}
                        if not table_data[d['table']].get(tbl.name):
                            table_data[d['table']][tbl.name] = {}
                        # print(d['table'], tbl.name, table_data, d)
                        table_data[d['table']][tbl.name] = d
                    result.close()
                    # FORM CUSTOMIZATION
                    tbl = Table('custom_form', _admin_metadata, autoload_with = _admin_engine)
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(
                            or_(
                                (and_(
                                    tbl.c.table.in_(tables),
                                    tbl.c.db == database,
                                    tbl.c.user_id == user.get('user_id'),
                                    tbl.c.app_id == app_id,
                                    tbl.c.excluded == False
                                )),
                                (and_(
                                    tbl.c.table.in_(tables),
                                    tbl.c.db == database,
                                    tbl.c.user_id == 1,
                                    tbl.c.app_id == app_id,
                                    tbl.c.excluded == False  
                                ))
                            )
                        ).\
                        order_by(tbl.c.custom_form_id.desc(), tbl.c.user_id.desc())#.\
                        #limit(1)
                    result = _admin_conn.execute(query)
                    data = result.mappings().all()
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        if not table_data.get(d['table']):
                            table_data[d['table']] = {}
                        if not table_data[d['table']].get(tbl.name):
                            table_data[d['table']][tbl.name] = {}
                        table_data[d['table']][tbl.name] = d
                    result.close()
                    # TABLE ACCESS INFORMATION
                    _access_instance = Access(self.conf, self.params, self.db, self.i18n)
                    access = await _access_instance.table_access(tables)
                    #print('admin access:', access)
                    if not access:
                        access = {}
                    output = {}
                    ref_tables = {}
                    # GENERATE OUTPUT
                    #print('_table:',table_data.get('_table'))
                    #print(database, app, app_id, tables)
                    for table in tables:
                        comment = table
                        if not table_data.get(table):
                            pass
                        elif table_data[table].get('translate_table'):
                            comment = table_data[table]['translate_table'].get('table_transl_desc')
                        else:
                            try:
                                comment = inspector.get_table_comment(table)['text']
                            except Exception as _err:# pylint: disable=broad-exception-caught
                                pass
                        output[table] = {
                            'table_id': None,
                            'table': table,
                            'comment': comment,
                            'database': database,
                            'fields': {}                         
                        }
                        if table_data.get('_table'):
                            if table_data['_table'].get(table):
                                output[table]['_table'] = table_data['_table'].get(table)
                                output[table]['table_id'] = table_data['_table'][table].get('table_id')
                        if table_data.get(table):
                            for t in table_data.get(table):
                                output[table][t] = table_data[table][t]
                        output[table]['permissions'] = access.get(table)
                        # FIELDS
                        try:
                            pks = {}
                            pk_constraint = inspector.get_pk_constraint(table)
                            if pk_constraint['constrained_columns']:
                                for _pk in pk_constraint['constrained_columns']:
                                    pks[_pk] = True
                                    output[table]['pk'] = _pk
                            fks = {}
                            for key in inspector.get_foreign_keys(table):
                                if key['constrained_columns']:
                                    for fk in key['constrained_columns']:
                                        key['constrained_column'] = key['constrained_columns'][0]
                                        del key['constrained_columns']
                                        key['referred_column'] = key['referred_columns'][0]
                                        del key['referred_columns']
                                        key['referred_columns_desc'] = None
                                        try:
                                            if not ref_tables.get(key['referred_table']):
                                                ref_tables_cols = [str(f.name) for f in metadata.tables[key['referred_table']].c]
                                                ref_tables[key['referred_table']] = copy.deepcopy(ref_tables_cols)
                                            else:
                                                ref_tables_cols = ref_tables[key['referred_table']]
                                            key['referred_columns_desc'] = ref_tables_cols[1]
                                        except Exception as _err:# pylint: disable=broad-exception-caught
                                            print('ERR PROCESSING FK KEY!!!', fk, table, key['referred_schema'], str(_err))
                                        fks[fk] = key
                            cols_specs = inspector.get_columns(table)
                            cols = [str(f.name) for f in metadata.tables[table].c]
                            for c in cols:
                                try:
                                    f = [col for col in cols_specs if col.get('name') == c][0]
                                    field_comment  = f.get('name')
                                    if f.get('comment'):
                                        field_comment  = f.get('comment')
                                    if not table_data.get(table):
                                        pass
                                    elif not table_data[table].get('translate_table_field'):
                                        pass
                                    elif table_data[table]['translate_table_field'].get(f.get('name')):
                                        field_comment = table_data[table]['translate_table_field'][f.get('name')].get('field_transl_desc')
                                except Exception as _err:# pylint: disable=broad-exception-caught
                                    pass
                                output[table]['fields'][f.get('name')] = {
                                    'name': f.get('name'),
                                    'pk': True if pks.get(f.get('name')) else False,
                                    'type': str(f.get('type')),
                                    'nullable': f.get('nullable'),
                                    'default': f.get('default'),
                                    'autoincrement': f.get('autoincrement'),
                                    'comment': field_comment,
                                    'computed': f.get('computed')
                                }
                                if fks.get(f.get('name')):
                                    output[table]['fields'][f.get('name')]['fk'] = True
                                    output[table]['fields'][f.get('name')]['ref'] = fks.get(f.get('name'))
                        except Exception as _err:# pylint: disable=broad-exception-caught
                            pass
                    conn.close()
                    engine.dispose()
                    try:
                        _admin_conn.close()
                        _admin_engine.dispose()
                    except Exception as _err2:# pylint: disable=broad-exception-caught
                        pass
                    return {
                        'success': True,
                        'msg': self.i18n('success'),
                        'data': output
                    }
                except Exception as _err:# pylint: disable=broad-exception-caught
                    try:
                        conn.close()
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
    async def get_apps(self):
        '''get apps'''
        try:
            engine, metadata, inspector, app, app_id, database, user, lang, admin_db = await self.commons()
            #print(engine, metadata, inspector, app, app_id, database, user)
            with engine.connect() as conn:
                try:
                    # APPS
                    roles = [user.get('role_id')]
                    # print('ROLES:',roles)
                    tbl = Table('app', metadata, autoload_with = engine)
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(and_(tbl.c.excluded == False))
                    if 1 not in roles:
                        join_tbl = Table('role_app', metadata, autoload_with = engine)
                        _join = join(tbl, join_tbl, tbl.c.app_id == join_tbl.c.app_id)
                        query = select(tbl.c).\
                            select_from(_join).\
                            where(
                                and_(
                                    join_tbl.c.role_id.in_(roles),
                                    join_tbl.c.access == True,
                                    tbl.c.excluded == False,
                                    join_tbl.c.excluded == False
                                )
                            )
                        query = select(tbl.c).\
                            select_from(tbl).\
                            where(
                                and_(
                                    tbl.c.app_id.in_(
                                        select(distinct(join_tbl.c.app_id)).\
                                        select_from(join_tbl).\
                                        where(
                                            and_(
                                                join_tbl.c.role_id.in_(roles),
                                                join_tbl.c.access == True,
                                                join_tbl.c.excluded == False
                                            )
                                        )
                                    )
                                ),
                                tbl.c.excluded == False
                            )
                    # print(str(query))                  
                    result = conn.execute(query)
                    data = result.mappings().all()
                    output = []
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        output.append(d)
                    result.close()
                    conn.close()
                    engine.dispose()
                    return {
                        'success': True,
                        'msg': self.i18n('success'),
                        'data': output
                    }
                except Exception as _err:# pylint: disable=broad-exception-caught
                    try:
                        conn.close()
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
    async def get_menu(self):
        '''get menu'''
        try:
            engine, metadata, inspector, app, app_id, database, user, lang, admin_db = await self.commons()
            #_admin_engine = engine
            #_admin_metadata = metadata
            if admin_db:
                engine = admin_db.get_engine()
                metadata = admin_db.get_metadata(engine)
            else:
                admin_db = self.db
            #_admin_conn = _admin_engine.connect()
            #print(engine, metadata, inspector, app, app_id, database, user)
            with engine.connect() as conn:
                try:
                    # APPS
                    roles = [user.get('role_id')]
                    #print('ROLES:',roles, app)
                    tbl = Table('menu', metadata, autoload_with = engine)
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(and_(
                            tbl.c.excluded == False,
                            tbl.c.app_id == app_id
                        ))
                    if not 1 in roles:
                        join_tbl = Table('role_app_menu', metadata, autoload_with = engine)
                        _join = join(tbl, join_tbl, and_(
                            tbl.c.app_id == join_tbl.c.app_id,
                            tbl.c.menu_id == join_tbl.c.menu_id
                        ))
                        query = select(tbl.c).\
                            select_from(_join).\
                            where(
                                and_(
                                    tbl.c.app_id == app_id,
                                    join_tbl.c.role_id.in_(roles),
                                    join_tbl.c.access == True,
                                    tbl.c.excluded == False,
                                    join_tbl.c.excluded == False
                                )
                            )
                        #print(str(query))                  
                    result = conn.execute(query)
                    data = result.mappings().all()
                    output = {}
                    menu = []
                    for row in data:
                        d = {}
                        for column in row:
                            d[column] = row[column]
                            if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                d[column] = d[column].isoformat()
                        menu.append(d)
                    # TABLES
                    tables = await self.get_tables(None)
                    output['tables'] = tables.get('data')
                    tbl = Table('menu_table', metadata, autoload_with = engine)
                    query = select(tbl.c).\
                        select_from(tbl).\
                        where(and_(
                            tbl.c.excluded == False,
                            tbl.c.app_id == app_id
                        ))
                    #print('ROLES:', roles)
                    if not 1 in roles:
                        join_tbl = Table('role_app_menu_table', metadata, autoload_with = engine)
                        _join = join(tbl, join_tbl, and_(
                            tbl.c.app_id == join_tbl.c.app_id,
                            tbl.c.menu_id == join_tbl.c.menu_id,
                            tbl.c.table_id == join_tbl.c.table_id
                        ))
                        query = select(tbl.c, join_tbl.c.create, join_tbl.c.read, join_tbl.c['update'], join_tbl.c.delete).\
                            select_from(_join).\
                            where(
                                and_(
                                    tbl.c.app_id == app_id,
                                    join_tbl.c.role_id.in_(roles),
                                    tbl.c.excluded == False,
                                    join_tbl.c.excluded == False,
                                    or_ (
                                        join_tbl.c.create ==  True,
                                        join_tbl.c.read ==  True
                                    )
                                )
                            )                        
                    #print('query:', query, app_id, roles)
                    result = conn.execute(query)
                    data = result.mappings().all()
                    # PROCESS MENU
                    for m in menu:
                        #print(m, data)
                        for row in data:
                            #print(m, row['menu_id'], m['menu_id'])
                            if row['menu_id'] == m['menu_id']:
                                d = {}
                                for column in row:
                                    d[column] = row[column]
                                    if isinstance(d[column], (datetime.datetime, datetime.date, datetime.time)):
                                        d[column] = d[column].isoformat()
                                d['menu'] = m['menu']
                                if not m.get('children'):
                                    m['children'] = []
                                if output.get('tables'):
                                    tbl = [t for t in output['tables'] if output['tables'][t].get('table_id') == d.get('table_id')]
                                    # print(tbl)
                                    if len(tbl) > 0:
                                        d['table'] = tbl[0]
                                m['children'].append(d)
                    output['menu'] = menu
                    result.close()
                    conn.close()
                    engine.dispose()
                    return {
                        'success': True,
                        'msg': self.i18n('success'),
                        'data': output
                    }
                except Exception as _err:# pylint: disable=broad-exception-caught
                    try:
                        conn.close()
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
    async def apps(self):
        '''apps'''
        return await self.get_apps()
    async def menu(self):
        '''menu'''
        return await self.get_menu()
    async def tables(self):
        '''tables'''
        return await self.get_tables()
    async def save_table_schema(self):
        '''save table schema'''
        try:
            engine, metadata, inspector, app, app_id, database, user, lang, admin_db = await self.commons()
            # print(engine, metadata, inspector, app, app_id, database, user, lang, admin_db)
            with engine.connect() as conn:
                try:
                    _sa_types_2_sql = {
                        'Integer': 'INTEGER',
                        'String': 'VARCHAR',
                        'Text': 'TEXT',
                        'Date': 'DATE',
                        'DateTime': 'DATETIME',
                        'Time': 'TIME',
                        'Float': 'DECIMAL',
                        'Boolean': 'BOOLEAN'
                    }
                    # print(self.params.get('data'))
                    if not self.params.get('data'):
                        return {'success': False, 'msg': self.i18n('no_data')}
                    elif not self.params['data'].get('table_metadata'):
                        return {'success': False, 'msg': self.i18n('no_table_metadata')}
                    elif not self.params['data']['table_metadata'].get('name'):
                        return {'success': False, 'msg': self.i18n('no_table_name')}
                    elif not self.params['data']['table_metadata'].get('comment'):
                        return {'success': False, 'msg': self.i18n('no_table_comment')}
                    elif not self.params['data'].get('fields'):
                        return {'success': False, 'msg': self.i18n('no_fields')}
                    elif len(self.params['data'].get('fields')) < 2:
                        return {'success': False, 'msg': self.i18n('table_must_have_2_or_more_fields')}
                    if self.params['data']['table_metadata'].get('name') in self.conf.get('CORE_TABLES'):
                        return {'success': False, 'msg': self.i18n('change_core_tables_not_allowed')}
                    Session = sessionmaker(bind = engine, autoflush = True)
                    session = Session()
                    Base = declarative_base()
                    table_name = self.params['data']['table_metadata'].get('name')
                    table_comment = self.params['data']['table_metadata'].get('comment')
                    table_id = self.params['data']['table_metadata'].get('table_id')
                    table_org_name = self.params['data']['table_metadata'].get('table_org') \
                        if self.params['data']['table_metadata'].get('table_org') \
                        else table_name
                    _fields = self.params['data'].get('fields')
                    schema = f"""class {table_name.capitalize()}(Base):
    __tablename__ = '{table_name}'
    __table_args__ = {{
        'extend_existing': True,
        'mysql_engine'   : 'InnoDB',
        'comment'        : '{table_comment}',
        'mysql_charset'  : 'utf8',
        'sqlite_autoincrement': True
    }}\n"""
                    #result.close()
                    #print('Column:', Column) 
                    _admin_engine = engine
                    _admin_metadata = metadata
                    if admin_db:
                        _admin_engine = admin_db.get_engine()
                        _admin_metadata = admin_db.get_metadata(_admin_engine)
                    else:
                        admin_db = self.db
                    if not table_id:
                        tbl = Table('table', _admin_metadata, autoload_with = _admin_engine)
                        query = select(tbl.c).\
                            select_from(tbl).\
                            where(and_(
                                tbl.c.db == database,
                                or_(
                                    tbl.c.table == table_name, 
                                    tbl.c.table == table_org_name
                                ),
                                tbl.c.excluded == False
                            )).\
                            order_by(desc(tbl.c.table_id))
                        with _admin_engine.connect() as _admin_conn:
                            result = _admin_conn.execute(query)
                            data = result.mappings().all()
                            # print(query, data, 'table_id', data['table_id'])
                            if len(data) > 0:
                                data = data[0]
                                self.params['data']['table_metadata']['table_id'] = data['table_id']
                                table_id = data['table_id']
                    # SQL TOP ALTER EXISTING TABLE
                    sqls = []
                    if table_name != table_org_name:
                        sqls.append(f"""ALTER TABLE "{table_org_name}"\n RENAME TO "{table_name}";""")
                    if not self.params['data']['table_metadata'].get('replace') \
                        and self.params['data']['table_metadata'].get('table_id'):
                        for _field in _fields:
                            _name = _field.get('name')
                            _type = _field.get('type')
                            _name_org = _field.get('field_org')
                            _dtype = _sa_types_2_sql.get(_type)
                            if _field.get('nchar'):
                                _dtype = f"{_dtype}({_field.get('nchar')})"
                            _nullable = '' if _field.get('nullable') is True else 'NOT NULL'
                            if _field.get('_added') is True:
                                sql = f"""ALTER TABLE "{table_name}"\n ADD "{_name}" {_dtype} {_nullable};"""
                                sqls.append(sql)
                            elif _field.get('_edited') is True:                            
                                if _name != _name_org:
                                    sql = f"""ALTER TABLE "{table_name}"\n RENAME COLUMN "{_name_org}" TO "{_name}";"""
                                    sqls.append(sql)
                                sql = f"""ALTER TABLE "{table_name}"\n MODIFY COLUMN "{_name}" {_dtype} {_nullable};"""
                                sqls.append(sql)
                            elif _field.get('_remove') is True:
                                sql = f"""ALTER TABLE "{table_name}"\n DROP COLUMN "{_name}";"""
                                sqls.append(sql)
                        #print(sqls)
                    if len(sqls) > 0:
                        for sql in sqls:
                            try:
                                conn.execute(sql)
                            except Exception as _err:# pylint: disable=broad-exception-caught
                                *_, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                print('DEBUG INF ALTER TABLE: ', str(_err), fname, exc_tb.tb_lineno, schema)
                                return {
                                    'success': False,
                                    'msg': self.i18n('unexpected-error', err = str(_err))
                                }
                    # DYNAMICALLY GEN SA MODEL
                    fks = []
                    for _field in _fields:
                        _name = _field.get('name')
                        _type = _field.get('type')
                        if _field.get('nchar'):
                            _type = f"{_type}({_field.get('nchar')})"
                        _primary_key = ', primary_key = True' if _field.get('primary_key') is True else ''
                        _autoincrement = ', autoincrement = True' if _field.get('autoincrement') is True else ''
                        _nullable = ', nullable = True' if _field.get('nullable') is True else ''
                        _unique = ', unique = True' if _field.get('unique') is True else ''
                        if _field.get('foreign_key'):
                            fks.append(_field.get('foreign_key'))
                        _foreign_key = f", ForeignKey('{_field.get('foreign_key')}')" if _field.get('foreign_key') else ''
                        _default = ''
                        if 'default' in _field and _type == 'Boolean':
                            _default = ', default = True' if _field.get('default') is True else ', default = False'
                        elif _field.get('default'):
                            _default = f""", default = '{_field.get('default')}'"""
                        _comment = _field.get('comment') if _field.get('comment') else _field.get('name')
                        schema += f"""    {_name} = Column({_type}{_primary_key}{_autoincrement}{_nullable}{_default}{_unique}{_foreign_key}, comment = "{_comment}")\n"""
                    # REFLECT THE FKS
                    clss = {}
                    globals_parameter  = {
                        'Table': Table, 
                        'Base': Base, 
                        'metadata': metadata, 
                        'engine': engine
                    }
                    locals_parameter = {}
                    for fk in fks:
                        _tbl = fk.split('.')[0]
                        _fld = fk.split('.')[1]
                        #_table = Table(_tbl, metadata, autoload_with = engine)
                        #fk_tbl = f"""class {_tbl.capitalize()}(Base):\n\t__tablename__ = '{_tbl}'\n\t__table__ = Table('{_tbl}', metadata, autoload_with = engine)\n"""
                        #fk_tbl = f"""class {_tbl.capitalize()}(Base):\n\t__table__ = _table\n"""
                        fk_tbl = f"""class {_tbl.capitalize()}(Base):\n\t__tablename__ = '{_tbl}'\n\t{_fld} = Column(Integer, primary_key = True)\n"""
                        #print(_table)
                        #print(fk_tbl)
                        ## comp = compile(fk_tbl, 'something', 'exec')
                        #exec(fk_tbl, globals_parameter , locals_parameter)
                        exec(fk_tbl) # pylint: disable=exec-used
                    #_run = 'Base.metadata.create_all(bind = engine, checkfirst = True)\n'
                    #print(f'{depend}{schema}{_run}')
                    try:
                        exec(f'{schema}') # pylint: disable=exec-used
                        #for t in Base.metadata.tables:
                        #    print(t)
                        if self.params['data']['table_metadata'].get('replace') is True:
                            eval(f'{table_name.capitalize()}.__table__.drop(bind = engine, checkfirst = True)')  # pylint: disable=eval-used
                        eval(f'{table_name.capitalize()}.__table__.create(bind = engine, checkfirst = True)') # pylint: disable=eval-used
                        #Base.metadata.create_all(bind = engine, checkfirst = True)                        
                        try:
                            _admin_conn.close()
                            _admin_engine.dispose()
                        except Exception as _err:# pylint: disable=broad-exception-caught
                            pass
                        try:
                            conn.close()
                            engine.dispose()
                            model_file = open(f'{os.getcwd()}/{self.conf.get("UPLOAD")}/{table_name.capitalize()}.mdl', mode = 'w', encoding = 'utf-8')
                            model_file.write(schema)
                            model_file.close()
                        except Exception as _err:# pylint: disable=broad-exception-caught
                            pass
                        # CREATE / UPDATE ADMIN TABLES WITH METADATA FOR INTERFACE
                        _data = copy.deepcopy(self.params['data'])
                        if table_id and not _data['_table'].get('table_id'):
                            _data['_table']['table_id'] = table_id
                        self.params['data'] = {'data': _data.get('_table'), 'table': 'table'}
                        self.params['db'] = copy.deepcopy(self.conf.get('DATABASE'))
                        _crud = Crud(self.conf, self.params, admin_db, self.i18n)
                        _upsert = await _crud.create_update()
                        # print('_table:', _data.get('_table'), _upsert)
                        _aux_data = [_data.get('table_metadata')]
                        _aux_data[0]['_table'] = 'translate_table'
                        if _upsert.get('inserted_primary_key'):
                            _aux_data[0]['table_id'] = _upsert.get('inserted_primary_key')
                        for field in _fields:
                            if _upsert.get('inserted_primary_key'):
                                field['table_id'] = _upsert.get('inserted_primary_key')
                            if table_id and not field.get('table_id'):
                                field['table_id'] = table_id
                            field['_table'] = 'translate_table_field'
                            _aux_data.append(field)
                        self.params['data']['data'] = _aux_data
                        return await _crud.create_update()
                    except Exception as _err:# pylint: disable=broad-exception-caught
                        *_, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print('DEBUG INF CREATE TABLE: ', str(_err), fname, exc_tb.tb_lineno, schema)
                        return {
                            'success': False,
                            'msg': self.i18n('unexpected-error', err = str(_err))
                        }
                    return {
                        'success': True,
                        'msg': self.i18n('success')
                    }
                except Exception as _err:# pylint: disable=broad-exception-caught
                    try:
                        conn.close()
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
        