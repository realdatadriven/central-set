'''DB'''
import os
import copy
import re
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.sql import text# pylint: disable=ungrouped-imports
from sqlalchemy.engine.url import URL
from sqlalchemy.dialects.mysql import base
base.ischema_names['tinyint'] = base.BOOLEAN
base.ischema_names['mediumtext'] = base.TEXT
base.ischema_names['longtext'] = base.TEXT

class DB:
    '''DB'''
    def __init__(self, conf, params):
        self.conf = conf
        self.params = params
    # ENGINE
    def get_engine(self, _database = None):
        'get SA engine'
        # RUN SPESSIFIC LOGIC FOR DIFERENT TYPES OF DB
        db_conf = copy.deepcopy(self.conf.get('DATABASE'))
        if _database:
            if isinstance(_database, dict):
                db_conf = copy.deepcopy(_database)
            else:
                db_conf['database'] = _database
        elif self.params.get('db_conf'):
            db_conf = copy.deepcopy(self.params.get('db_conf'))
        elif self.params.get('database'):
            if isinstance(self.params.get('database'), dict):
                db_conf = self.params.get('database')
            else:
                db_conf['database'] = self.params.get('database')
        elif self.params.get('db'):
            if isinstance(self.params.get('db'), dict):
                db_conf = self.params.get('db')
            else:
                db_conf['database'] = self.params.get('db')
        elif self.params.get('app', {}).get('db'):
            if isinstance(self.params['app'].get('db'), dict):
                db_conf = self.params['app'].get('db')
            else:
                db_conf['database'] = self.params['app'].get('db')
        elif self.params.get('app', {}).get('database'):
            if isinstance(self.params['app'].get('database'), dict):
                db_conf = self.params['app'].get('database')
            else:
                db_conf['database'] = self.params['app'].get('database')
        if isinstance(db_conf, dict):
            _patt = re.compile(r'@ENV\..+')
            for _key in db_conf:
                match_env = re.findall(_patt, str(db_conf[_key]))
                if len(match_env) > 0:
                    _env = re.sub(r'@ENV\.', '', str(match_env[0]))
                    try:
                        db_conf[_key] = os.environ.get(_env)
                    except Exception as _err:# pylint: disable=broad-exception-caught
                        pass
        connect_args = {}
        _db_basename, _db_ext = os.path.splitext(db_conf['database'])
        if db_conf['drivername'] == 'duckdb' or _db_ext in ['.duckdb', '.ddb']:
            db_conf['drivername'] = 'duckdb'
            connect_args = {
                'read_only': False,
                'config': {
                    'memory_limit': '500mb'
                }
            }
            connect_args = {}
            database = db_conf['database']
            _db_path, _db_file = os.path.split(database)
            _db_basename, _db_ext = os.path.splitext(database)
            if not _db_path and _db_file:
                db_conf['database'] = f'{os.getcwd()}/database/{database}'
            if not _db_ext and _db_basename:
                db_conf['database'] = f'{database}.duckdb'
            url = URL.create(**db_conf)
        elif db_conf['drivername'] == 'sqlite' or _db_ext in ['.db', '.sqlite', '.sqlite3']:
            connect_args = {'check_same_thread': False, 'timeout': 150}
            exists = os.path.isfile(db_conf['database'])
            if exists:
                pass
            else:
                if self.params.get('sqlite_specific_path'):
                    db_conf['database'] = f'{os.getcwd()}/{self.params.get("sqlite_specific_path")}/{db_conf["database"]}'
                else:
                    db_conf['database'] = f'{os.getcwd()}/database/{db_conf["database"]}'
            basename, extension = os.path.splitext(db_conf['database'])
            if extension not in ['.db'] and basename:
                db_conf['database'] = f'{db_conf["database"]}.db'
            url = URL.create(**db_conf)
        else:
            if db_conf.get('drivername') not in ['sqlite', 'pysqlite', 'duckdb']:
                if db_conf.get('drivername').find('mysql') != -1 and db_conf.get('database'):
                    try:
                        aux_conf = copy.deepcopy(db_conf)
                        del aux_conf['database']
                        url = URL.create(**aux_conf)
                    except Exception as _err:# pylint: disable=broad-exception-caught
                        print('CREATE DB IF NOT EXIXTS', str(_err))
            url = URL.create(**db_conf)
        # print(db_conf)
        engine = create_engine(url, echo = self.conf.get('DB_LOG'), connect_args = connect_args)
        # CREATE THE DB IF NOT EXISTS
        if db_conf.get('drivername') not in ['sqlite', 'duckdb']:
            if not database_exists(engine.url):
                create_database(engine.url)
        if db_conf['drivername'] == 'sqlite':
            with engine.connect() as conn:
                conn.execute(text('PRAGMA journal_mode = wal2'))
                conn.execute(text('PRAGMA foreign_keys = ON'))
                conn.execute(text('PRAGMA secure_delete = ON'))
                #cache_size = -500 * 1024
                cache_size = 2 * 1024 * 1024
                conn.execute(text(f'PRAGMA cache_size = {(-1 * cache_size)}'))
                conn.execute(text(f'PRAGMA PAGE_SIZE = {cache_size}'))
                # conn.execute(text('PRAGMA mmap_size  = {}'.format(500 * 1024)))
                conn.execute(text('PRAGMA synchronous = 0'))
                conn.execute(text('PRAGMA TEMP_STORE  = 2'))
                conn.execute(text('PRAGMA auto_vacuum = FULL'))
                busy_timeout = 60 * 1000 # 60s / 1m
                conn.execute(text(f'PRAGMA busy_timeout = {busy_timeout}'))
            # https://www.sqlite.org/cgi/src/doc/begin-concurrent/doc/begin_concurrent.md
            #engine.execute('BEGIN CONCURRENT')
        elif db_conf['drivername'] == 'mysql':
            try:
                with engine.connect() as conn:
                    conn.execute(text("SET GLOBAL sql_mode = 'ANSI';"))
            except Exception as _e:# pylint: disable=broad-exception-caught
                pass
        return engine
    # METADATA
    def get_metadata(self, engine):# pylint: disable=unused-argument
        '''get SA metadata'''
        metadata = MetaData()
        #metadata.reflect(engine)
        return metadata
    # INSPECTOR
    def get_inspector(self, engine):
        '''get SA inspector'''
        return inspect(engine)
    # GET CONSTRAINT
    async def get_constraint(self, metadata, inspector, table):
        '''get constraint returns the PK and the FK's'''
        pks = {}
        fks = {}
        pk = None
        try:
            ref_tables = {}
            pk_constraint = inspector.get_pk_constraint(table)
            if pk_constraint['constrained_columns']:
                for _pk in pk_constraint['constrained_columns']:
                    if not pk:
                        pk = _pk
                    pks[_pk] = True
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
        except Exception as _err:# pylint: disable=broad-exception-caught
            print(str(_err))
        return (pk, fks)
    # SESSION
    def get_session(self, engine):# pylint: disable=unused-argument
        '''get SA session'''
        return None
# dbC = DB({'db_conf':{'drivername': 'sqlite', 'database': 'SA_ADMIN.db'}})
# en = dbC.getEngine()
