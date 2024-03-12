'''EXPOT DUCKDB WAY'''
# pylint: disable=unused-variable
# pylint: disable=line-too-long
# pylint: disable=wrong-import-order
# pylint: disable=broad-exception-caught
# pylint: disable=unused-import
# pylint: disable=invalid-name
# pylint: disable=trailing-whitespace
import os
import sys
import tempfile
import datetime
import copy
import re
import json
import zipfile
from dateutil import parser
from query_doc import QueryDoc
import duckdb

class ExportDucdb:
    '''EXPOTS DUCKDB'''
    def __init__(self, conf, params, db, i18n):
        self.conf = conf
        self.params = params
        self.db = db
        self.i18n = i18n
    async def export(self):
        '''DUCKDB EXPORT
        ```json
        {
            ...,
            "data": {
                "database|db": "path/to/db",
                "file": "<fname>",
                "sql|query": "SELECT | COPY (SELECT|TABLE) TO <fname>",
                "conf": {"extentions": [], "format": ".format"}
            }
        }```
        OR
        ```json
        {
            ...,
            "data": {
                "database|db": "path/to/db",
                "file": "<fname>",
                "data": [
                    {
                        "sql|query": "SELECT | COPY (SELECT|TABLE) TO <fname>",
                        "conf": {"extentions": [], "format": ".format"}
                    }
                ]
            }
        }
        ```
        '''
        try:
            _data = self.params['data']
            _conf = _data.get('_conf', _data.get('conf', {}))
            _etlrb = _data.get('selected_etlrb', _conf)
            _export = _data.get('export', {})
            _details = _data.get('data', [])
            fname = _data.get('file')
            compress = _data.get('compress', _conf.get('compress'))
            compress_format = _data.get('compress_format', _conf.get('compress_format'))
            # template = _data.get('template')
            date_ref = _data.get('date_ref') if isinstance(_data.get('date_ref'), (datetime.datetime, datetime.date)) else parser.parse(_data.get('date_ref', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            basename, ext = os.path.splitext(fname)
            if compress_format and compress:
                fname = f'{fname}.{compress_format}'
            upload_path = self.conf.get('UPLOAD')
            tmp = 'tmp'
            try:
                os.stat(f'{os.getcwd()}/{upload_path}/{tmp}')
            except FileExistsError as _err:
                os.mkdir(f'{os.getcwd()}/{upload_path}/{tmp}')
            # _path = f'{os.getcwd()}/{upload_path}/{tmp}/{fname}'
            _path = f'{os.getcwd()}/{upload_path}/{tmp}'            
            _path = os.path.normpath(_path).encode("unicode_escape").decode("utf8")
            _tempdir = tempfile.gettempdir()
            database = _data.get('db', _data.get('database', _export.get('database', _etlrb.get('database'))))
            _db_path, _db_file = os.path.split(database)
            _db_basename, _db_ext = os.path.splitext(database)
            if not _db_path and _db_file:
                database = f'{os.getcwd()}/database/{database}'
            if not _db_ext and _db_basename:
                database = f'{database}.duckdb'
            _qd = QueryDoc(query_parts = {})
            # conn = duckdb.connect(database, read_only = False, config = {'memory_limit': '500mb'})
            conn = duckdb.connect(database, read_only = False, config = {})
            if not len(_details) > 0:
                # LOAD EXTENTIONS
                _extras = ''
                if ext in ['.xlsx'] and not _conf.get('extentions'):
                    _conf['extentions'] = ['spatial']                
                if not _conf.get('extentions'):
                    pass
                elif isinstance(_conf.get('extentions'), list):
                    for extention in _conf.get('extentions'):
                        print(extention)
                        try:
                            conn.install_extension(extention)
                        except Exception as _err:
                            #conn.close()
                            *_, exc_tb = sys.exc_info()
                            py_fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print('DEBUG INF DUCKDB EXTENTIONS INSTALL: ', extention, str(_err), py_fname, exc_tb.tb_lineno)
                            #return {'success': False, 'msg': self.i18n('duckdb-extention-faild', extention = extention, err = str(_err))}
                        try:
                            conn.load_extension(extention)
                        except Exception as _err:
                            #conn.close()
                            *_, exc_tb = sys.exc_info()
                            py_fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print('DEBUG INF DUCKDB EXTENTIONS LOAD: ', extention, str(_err), py_fname, exc_tb.tb_lineno)
                            #return {'success': False, 'msg': self.i18n('duckdb-extention-faild', extention = extention, err = str(_err))}
                # SQL
                sql = _data.get('sql', _data.get('query'))
                sql = _qd.set_date(sql, dates = date_ref)
                patt = r'COPY.+?\(.+\).+TO+.\'.+\''
                _has_copy_already = re.findall(patt, re.sub(r'\n', ' ', sql))
                filename = fname
                _export_full_path = f'{_path}/{filename}'  
                if len(_has_copy_already) > 0:
                    pass
                elif ext in ['.xlsx']:
                    sql = f"""COPY ({sql}) TO '<fname>' WITH (FORMAT GDAL, DRIVER 'xlsx')"""
                    if os.path.exists(_export_full_path):
                        try:
                            os.remove(_export_full_path)
                        except FileNotFoundError:
                            print(f"{_export_full_path} not found.")
                        except Exception as e:
                            print(f"An error occurred: {str(e)}")
                else:
                    sql = f"""COPY ({sql}) TO '<fname>'"""           
                #_path = os.path.normpath(_path).encode("unicode_escape").decode("utf8")
                _format = _conf.get('format', '.csv')
                sql = re.sub(r'<filename>|<fname>', _export_full_path, sql)
                sql = _qd.set_date(sql, date_ref)
                try:
                    print(sql)
                    conn.sql(sql)
                    conn.close()
                except Exception as _err:
                    conn.close()
                    *_, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print('DEBUG INF DUCKDB EXPORT QUERY: ', str(_err), fname, exc_tb.tb_lineno)
                    return {'success': False, 'msg': self.i18n('duckdb-import-faild', sql = sql, err = str(_err))}
                # print(fname)
                return {'success': True, 'msg': self.i18n('success'), 'fname': f'tmp/{fname}'}
            else:
                _exps = []
                for _detail in _details:
                    # CONFIGURATION
                    _dtail_conf = _detail.get('etl_rb_exp_dtail_conf', _detail.get('_conf', _detail.get('conf', {})))
                    if _dtail_conf:
                        try:
                            if isinstance(_dtail_conf, str):
                                _dtail_conf = json.loads(_dtail_conf)
                            elif isinstance(_dtail_conf, dict):
                                pass
                        except Exception as _err:# pylint: disable=broad-exception-caught
                            print(str(_err))
                    _dtail_conf = {} if not _dtail_conf else _dtail_conf
                    # print('_dtail_conf:', type(_dtail_conf), _dtail_conf)
                    # LOAD EXTENTIONS
                    if ext in ['.xlsx'] and not _dtail_conf.get('extentions'):
                        _dtail_conf['extentions'] = ['spatial']
                    if not _dtail_conf.get('extentions'):
                        pass
                    elif isinstance(_dtail_conf.get('extentions'), list):
                        for extention in _dtail_conf.get('extentions'):
                            try:
                                conn.install_extension(extention)
                                conn.load_extension(extention)
                            except Exception as _err:
                                conn.close()
                                *_, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                print('DEBUG INF DUCKDB EXTENTIONS: ', str(_err), fname, exc_tb.tb_lineno)
                                return {'success': False, 'msg': self.i18n('duckdb-extention-faild', extention = extention, err = str(_err))}
                    # SQL
                    sql = _detail.get('sql_export_query', _detail.get('sql', _detail.get('query')))
                    sql = _qd.set_date(sql, dates = date_ref)
                    patt = r'COPY.+?\(.+\).+TO+.\'.+\''
                    _has_copy_already = re.findall(patt, re.sub(r'\n', ' ', sql))
                    if len(_has_copy_already) > 0:
                        pass
                    else:
                        sql = f"""COPY ({sql}) TO '<fname>'"""
                    filename = fname
                    _export_full_path = f'{_path}/{filename}'
                    _format = _dtail_conf.get('format', '.csv')
                    details_to_tmp = _dtail_conf.get('details_to_tmp', True)
                    if _etlrb.get('each_details_on_its_own_file') is True or len(_details) > 1:
                        filename = f'{_detail.get("etl_rb_exp_dtail")}_YYYYMMDD{_format}'
                        _export_full_path = f'{_path}/{filename}'
                        if details_to_tmp is True:
                            _export_full_path = f'{_tempdir}/{filename}'
                    _export_full_path = _qd.set_date(_export_full_path, date_ref)
                    sql = re.sub(r'<filename>|<fname>', _export_full_path, sql)
                    sql = _qd.set_date(sql, date_ref)
                    try:
                        print(sql, date_ref)
                        conn.sql(sql)
                        _exps.append({'success': True, 'msg': self.i18n('success'), 'fname': f'{_export_full_path}'})
                    except Exception as _err:
                        conn.close()
                        *_, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print('DEBUG INF DUCKDB EXPORT QUERY: ', str(_err), fname, exc_tb.tb_lineno)
                        return {'success': False, 'msg': self.i18n('duckdb-import-faild', sql = sql, err = str(_err))}
                conn.close()   
                # ZIP ALL THE DETAILS WITH THE EMPLATE
                if _etlrb.get('each_details_on_its_own_file') is True or len(_details) > 1:
                    template = _export.get('attach_file_template')
                    basename, ext = os.path.splitext(template)
                    basename = _qd.set_date(basename, date_ref)
                    upload_path = self.conf.get('UPLOAD')
                    zped_path = f'{os.getcwd()}/{upload_path}/tmp/{basename}.zip'
                    _zf = zipfile.ZipFile(zped_path, 'w', zipfile.ZIP_DEFLATED)
                    if os.path.exists(f'{os.getcwd()}/{upload_path}/{template}'):
                        _zf.write(f'{os.getcwd()}/{upload_path}/{template}', f'{basename}.{ext}')
                    for _exp in _exps:
                        _export_full_path = _exp.get('fname')
                        _, fname = os.path.split(_export_full_path)
                        _zf.write(_export_full_path, fname)
                    _zf.close()
                    return {'success': True, 'msg': self.i18n('success'), 'fname': f'tmp/{basename}.zip'}
                else:
                    _path, fname = os.path.split(_exps[0]['fname'])
                    _exps[0]['fname'] = f'tmp/{fname}'
                    return _exps[0]
            # conn.close()
        except Exception as _err:# pylint: disable=broad-exception-caught
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
