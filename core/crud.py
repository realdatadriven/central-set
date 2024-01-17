'''CRUD'''
import os
import sys
import copy 
import datetime
from dateutil import parser
import icalendar
#from icalevents.icalevents import events
from icalevents import icalparser


from core.access import Access
from core.read import Read
from core.create_update import CreateUpdate
from core.query import Query

class Crud:
    '''CRUD'''
    def __init__(self, conf, params, db, i18n):
        self.conf = conf
        self.params = params
        self.db = db
        self.i18n = i18n
    #CHECK TABLE PERMISSIONS
    def check_table_access(self, table, permissions, crud_action):
        '''check table access'''
        user = self.params.get('user')
        print(table, crud_action, permissions[table].get(crud_action))
        if user.get('role_id') != 1:
            if not permissions.get(table):
                return {'success': False, 'msg': self.i18n('no-table-access', table = table)}
            elif permissions[table].get(crud_action) is False \
                or permissions[table].get(crud_action) == 0 \
                or not permissions[table].get(crud_action):
                return {'success': False, 'msg': self.i18n('no-table-action-access', table = table, action = crud_action)}
        return {'success': True}
    # CREATE UPDATE
    async def create_update(self):
        '''CREATE / UPDATE'''
        try:
            user = self.params.get('user')
            _table = self.params['data'].get('table')
            if not _table:
                return {'success': False, 'msg': self.i18n('no-table')}
            # PERMISSIONS
            self.params['permissions'] = {}
            if user.get('role_id') != 1:
                _access = Access(self.conf, self.params, self.db, self.i18n)
                permissions = await _access.permissions(_table)
                self.params['permissions'] = permissions
                # ROW LEVEL ACCESS
                row_level_tables = await _access.row_level_tables()
                # print(row_level_tables)
                row_level_tables = row_level_tables.get('tables')
                if len(row_level_tables) > 0:
                    self.params['row_level_tables'] = row_level_tables
            _create_update = CreateUpdate(self.conf, self.params, self.db, self.i18n)
            _data = self.params['data'].get('data')
            #print(_table, _data)
            if not _data:
                return {'success': False, 'msg': self.i18n('no-data')}
            if isinstance(_data, list) and len(_data) > 1:
                _data_list = copy.deepcopy(_data)
                output = {}
                i = 0
                for d in _data_list:
                    i += 1
                    table = d.get('_table') if d.get('_table') else _table
                    # print(i, table)
                    output[f'{table}_row_exec_{str(i)}'] = await _create_update.run(table, d)
                return {'success': True, 'msg': self.i18n('success'), 'data': output}
            elif isinstance(_data, list) and len(_data) > 0:
                return await _create_update.run(_table, _data[0])
            else:
                return await _create_update.run(_table, _data)
        except Exception as _err:# pylint: disable=broad-exception-caught
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
    # READ
    async def read(self):
        '''READ'''
        try:
            user = self.params.get('user')
            _table = self.params['data'].get('table')
            # PERMISSIONS
            self.params['permissions'] = {}
            if user.get('role_id') != 1:      
                _access = Access(self.conf, self.params, self.db, self.i18n)
                permissions = await _access.permissions(_table)
                self.params['permissions'] = permissions
                # ROW LEVEL ACCESS
                row_level_tables = await _access.row_level_tables()
                #print('row_level_tables:', row_level_tables)
                row_level_tables = row_level_tables.get('tables')
                if len(row_level_tables) > 0:
                    self.params['row_level_tables'] = row_level_tables
            _read = Read(self.conf, self.params, self.db, self.i18n)
            #print('table:', _table)
            if not _table:
                return {'success': False, 'msg': self.i18n('no-table')}
            if isinstance(_table, list) and len(_table) > 1:
                tables = copy.deepcopy(_table)
                data = {}
                for table in tables:
                    data[table] = await _read.run(table)
                return {'success': True, 'msg': self.i18n('success'), 'data': data}
            elif isinstance(_table, list) and len(_table) > 0:
                return await _read.run(_table[0])
            else:
                return await _read.run(_table)
        except Exception as _err:# pylint: disable=broad-exception-caught
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
    # QUERY
    async def query(self):
        '''Executes a query'''
        try:
            _data = self.params.get('data')
            _query = Query(self.conf, self.params, self.db, self.i18n)
            if not _data.get('build_query') and _data.get('query'):
                if self.conf.get('ALLOW_CLI_RUN_QUERIES'):
                    engine = self.db.get_engine()
                    return await _query.exec_query(engine, _data.get('query'))
                else:
                    return {'success': False, 'msg': self.i18n('run-query-string-not-allowed')}
            elif _data.get('use_query_string') and not self.conf.get('ALLOW_CLI_RUN_QUERIES'):
                return {'success': False, 'msg': self.i18n('run-query-string-not-allowed')}
            elif _data.get('use_query_string') and not _data.get('query'):
                return {'success': False, 'msg': self.i18n('no-query-string')}
            elif _data.get('use_query_string') and _data.get('query'):
                engine = self.db.get_engine()
                if isinstance(_data.get('query'), dict):
                    data = {}
                    for _key in _data.get('query'):
                        data[_key] = await _query.exec_query(engine, _data['query'].get(_key))
                    return {'success': True, 'msg': self.i18n('success'), 'data': data}
                else:
                    return await _query.exec_query(engine, _data.get('query'))           
            else:
                user = self.params.get('user')
                _build_query = _data.get('build_query')
                if isinstance(_build_query, dict):
                    data = {}
                    for _key in _build_query:
                        if isinstance(_build_query[_key], list):
                            _tables = list(set(map(lambda d: d.get('table'), _build_query[_key])))
                            # PERMISSIONS
                            self.params['permissions'] = {}
                            if user.get('role_id') != 1:
                                _access = Access(self.conf, self.params, self.db, self.i18n)
                                permissions = await _access.permissions(_tables)
                                self.params['permissions'] = permissions
                                # ROW LEVEL ACCESS
                                row_level_tables = await _access.row_level_tables()
                                row_level_tables = row_level_tables.get('tables')
                                if len(row_level_tables) > 0:
                                    self.params['row_level_tables'] = row_level_tables
                            #print('table:', _tables)
                            if not _tables:
                                data[_key] = {'success': False, 'msg': self.i18n('no-table')}
                            elif len(_tables) == 0:
                                data[_key] = {'success': False, 'msg': self.i18n('no-table')}
                            else:
                                data[_key] = await _query.run(_tables, _build_query[_key])
                    return {'success': True, 'msg': self.i18n('success'), 'data': data}
                else:
                    if isinstance(_build_query, list):
                        _tables = list(set(map(lambda d: d.get('table'), _build_query)))
                        # PERMISSIONS
                        self.params['permissions'] = {}
                        if user.get('role_id') != 1:   
                            _access = Access(self.conf, self.params, self.db, self.i18n)
                            permissions = await _access.permissions(_tables)
                            self.params['permissions'] = permissions
                            # ROW LEVEL ACCESS
                            row_level_tables = await _access.row_level_tables()
                            row_level_tables = row_level_tables.get('tables')
                            if len(row_level_tables) > 0:
                                self.params['row_level_tables'] = row_level_tables
                        #print('table:', _tables)
                        if not _tables:
                            return {'success': False, 'msg': self.i18n('no-table')}
                        elif len(_tables) == 0:
                            return {'success': False, 'msg': self.i18n('no-table')}
                        return await _query.run(_tables, _build_query)
        except Exception as _err:# pylint: disable=broad-exception-caught
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
    # PROCESS iCals
    async def parse_icals(self):
        '''Parse list of ical in to a list of events'''
        try:
            # print(self.params.get('data'))
            _events_obj = []
            _evnts = self.params.get('data', {}).get('events', [])
            _days = self.params.get('data', {}).get('days', 30)
            _header = self.params.get('data', {}).get('e_header')
            _tail = self.params.get('data', {}).get('e_tail')
            start = None
            if self.params.get('data', {}).get('start'):
                try:
                    start = parser.parse(self.params.get('data', {}).get('start'))
                except Exception as _err:# pylint: disable=broad-exception-caught
                    print(str(_err))
            end = None
            if self.params.get('data', {}).get('end'):
                try:
                    end = parser.parse(self.params.get('data', {}).get('end'))
                except Exception as _err:# pylint: disable=broad-exception-caught
                    print(str(_err))
            # print(start, end)
            for _evnt in _evnts:
                ical = f"{_header}\n{_evnt.get('ical')}\n{_tail}"
                try:
                    parsed_events = icalparser.parse_events(
                        ical,
                        start = start,
                        end = end,
                        default_span = datetime.timedelta(days = _days)
                    )
                    #print(1, _evnt.get('task_id'), len(parsed_events))
                    for _ical in parsed_events:
                        try:
                            calendar = icalendar.Calendar.from_ical(ical)
                            for event in calendar.walk('VEVENT'):
                                # print(event.get("SUMMARY"), event.get("DTSTART").dt, event.get("DTEND").dt)
                                if _ical.summary == event.get("SUMMARY"):
                                    alarms = event.walk("VALARM")
                                    _alarms_obj = []
                                    for alarm in alarms:
                                        trigger_value = alarm.get("TRIGGER")
                                        #_dt = event.get("DTSTART").dt + trigger_value.dt
                                        _dt = _ical.start + trigger_value.dt
                                        _alarms_obj.append({
                                            'description': alarm.get("DESCRIPTION"),
                                            'trigger': _dt
                                        })
                            _events_obj.append({
                                **_evnt,
                                'summary': _ical.summary,
                                'start': _ical.start,
                                'alarms': _alarms_obj
                            })
                        except Exception as _err:# pylint: disable=broad-exception-caught
                            print(2, _evnt.get('task_id'), str(_err))
                            _events_obj.append({
                                **_evnt,
                                'success': False,
                                'msg': str(_err),
                                'summary': None,
                                'start': None,
                                'alarms': []
                            })
                except Exception as _err:# pylint: disable=broad-exception-caught
                    print(1, _evnt.get('task_id'), str(_err))
                    _events_obj.append({
                        **_evnt,
                        'success': False,
                        'msg': str(_err),
                        'summary': None,
                        'start': None,
                        'alarms': []
                    })
            return {'success': True, 'msg': self.i18n('success'), 'data': _events_obj}
        except Exception as _err:# pylint: disable=broad-exception-caught
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
    # AUXS
    async def create(self):
        '''CREATE'''
        return await self.create_update()
    async def upsert(self):
        '''UPSERT (INSERT | UPDATE)'''
        return await self.create_update()
    async def update(self):
        '''UPDATE'''
        return await self.create_update()
    async def delete(self):
        '''DELETE'''
        try:
            if not self.params.get('data'):
                pass
            elif self.params['data'].get('data'):
                if isinstance(self.params['data'].get('data'), list):
                    for d in self.params['data']['data']:
                        d['_to_delete'] = True
                else:
                    self.params['data']['data']['_to_delete'] = True
        except Exception as _err:# pylint: disable=broad-exception-caught
            pass
        return await self.create_update()
    async def c(self):
        '''CREATE'''
        return await self.create()
    async def r(self):
        '''READ'''
        return await self.read()
    async def u(self):
        '''UPDATE'''
        return await self.update()
    async def d(self):
        '''DELETE'''
        return await self.delete()