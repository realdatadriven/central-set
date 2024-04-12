
'''main'''
# pylint: disable=unused-variable
# pylint: disable=line-too-long
# pylint: disable=wrong-import-order
# pylint: disable=broad-exception-caught
# pylint: disable=unused-import
# pylint: disable=invalid-name
# pylint: disable=trailing-whitespace
# pylint: disable=ungrouped-imports
import os
import sys
import datetime
import json
import copy
import tempfile
import importlib
import re
from typing import Optional, List, Union
import uvicorn
from uvicorn.config import LOGGING_CONFIG
import yaml

import i18n
from fastapi import Body, FastAPI, File, UploadFile, Security, Request, Cookie, Query, status #, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel # pylint: disable=no-name-in-module
#from fastapi.security import OAuth2PasswordBearer
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt
from werkzeug.utils import secure_filename
from fastapi.testclient import TestClient
from fastapi.websockets import WebSocket, WebSocketDisconnect
from dotenv import load_dotenv

# CORE CODE
from core.db import DB
from core.login import Login
from core.crud import Crud
#from core.admin import Admin
#from core import *

load_dotenv()

app = FastAPI()
#oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
security = HTTPBearer(auto_error = False)

# CORS
#https://fastapi.tiangolo.com/tutorial/cors/
origins = [ '*' ] #'http://localhost', 'http://localhost:8080'
app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials = True,
    allow_methods = ['*'],
    allow_headers = ['*'],
)

class Req(BaseModel): # pylint: disable=missing-class-docstring
    lang: Optional[str] = 'en'
    data: Optional[dict] = {}
    app: Optional[dict] = {}

class Res(BaseModel):# pylint: disable=missing-class-docstring
    sucess: bool
    msg: str
    data: Optional[dict]

i18n.load_path.append('locales')
i18n.set('filename_format', '{locale}.{format}')
i18n.set('locale', 'en')
i18n.set('fallback', 'en')

# CONFIG FILE
conf = {}
with open('core/conf.yml', mode = 'r', encoding = 'utf-8') as config_file:
    conf = yaml.safe_load(config_file)
# conf ={**conf, **os.environ}
# INDEX
@app.get('/')
def index(_q: Optional[str] = None):
    '''entriepoint'''
    _f = open('static/index.html', mode = 'r', encoding = 'utf-8')
    html = _f.read()
    _f.close()
    return HTMLResponse(content = html, status_code = 200)

#STATIC FILES
#https://fastapi.tiangolo.com/tutorial/static-files/
try:
    os.stat(f'{os.getcwd()}/{conf.get("STATIC")}')
except Exception as _err:# pylint: disable=broad-exception-caught
    os.mkdir(f'{os.getcwd()}/{conf.get("STATIC")}')
try:
    os.stat(f'{os.getcwd()}/{conf.get("UPLOAD")}')
except Exception as _err:# pylint: disable=broad-exception-caught
    os.mkdir(f'{os.getcwd()}/{conf.get("UPLOAD")}')
app.mount('/static', StaticFiles(directory = conf.get('STATIC')), name = 'static')
app.mount('/assets', StaticFiles(directory = conf.get('ASSETS')), name = 'static')
app.mount('/uploads', StaticFiles(directory = conf.get('UPLOAD')), name = 'static')

# VERIFY TOKEN
def verify_token(token: str) -> dict:
    '''doc'''
    try:
        payload = jwt.decode(
            token,
            conf.get('SECRET_KEY'),
            algorithms = [conf.get('ALGORITHM')]
        )
        return {
            'success': True,
            'msg': i18n.t('success-token'),
            'payload': payload
        }
    except Exception as _err:# pylint: disable=broad-exception-caught
        err = re.sub(re.compile(r'\.$'), '', str(_err))
        return {
            'success': False,
            'msg': i18n.t('invalid-token', err = err),
            'err': err
        }
#UPLOADS <form enctype='multipart/form-data' + python-multipart
#https://fastapi.tiangolo.com/tutorial/request-files/ 
@app.post('/uploadfiles/')
async def upload_files(files: List[UploadFile] = File(...),
    tmp: Union[bool, None] = Body(default = None),
    path: Union[str, None] = Body(default = None),
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security)
):
    '''handle multiple files upload '''
    try:
        token = None
        if credentials:
            token = credentials.credentials
        verify = verify_token(token)
        if verify.get('success') is False:
            return verify
        upload_path = path if path else conf.get('UPLOAD')
        try:
            os.stat(f'{os.getcwd()}/{upload_path}')
        except FileNotFoundError as _err:
            os.mkdir(f'{os.getcwd()}/{upload_path}')
        _files = []
        for file in files:
            file_content = await file.read()
            filename, extention = os.path.splitext(file.filename)
            if tmp is True:
                upload_path = tempfile.gettempdir()
                _f = open(f'{upload_path}/{filename}{extention}', 'wb')
                _f.write(file_content)
                _f.close()
            else:
                for i in range(0,100):
                    filename, extention = os.path.splitext(file.filename)
                    if i > 0:
                        filename = secure_filename(f'{filename}({i})')
                    else:
                        filename = secure_filename(filename)
                    file_exists = os.path.exists(f'{upload_path}/{filename}{extention}')
                    if not file_exists:
                        _f = open(f'{upload_path}/{filename}{extention}', 'wb')
                        _f.write(file_content)
                        _f.close()
                        break
                    else:
                        continue
            _files.append({
                'success': True,
                'msg': i18n.t('file-success', file = f'{filename}{extention}'),
                'file': f'{filename}{extention}'
            })
        return {
            'success': True,
            'msg': i18n.t('files-success'),
            'files': _files
        }
    except Exception as _err:# pylint: disable=broad-exception-caught
        *_, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
        return {
            'success': False,
            'msg': i18n.t('unexpected-error', err = str(_err))
        }
@app.post('/upload')
@app.post('/upload/')
async def upload_file(
    #req_all: Request,
    #Authorization: Union[str, None] = Header(default= None),
    file: Optional[UploadFile] = File(...),
    tmp: Union[bool, None] = Body(default = None),
    path: Union[str, None] = Body(default = None), #pylint: disable=unused-argument
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security)
):
    '''handle single file upload'''
    try:
        token = None
        if credentials:
            token = credentials.credentials
        verify = verify_token(token)
        if verify.get('success') is False:
            return verify
        file_content = await file.read()
        upload_path = conf.get('UPLOAD')
        try:
            os.stat(f'{os.getcwd()}/{upload_path}')
        except FileNotFoundError as _err:
            os.mkdir(f'{os.getcwd()}/{upload_path}')
        filename, extention = os.path.splitext(file.filename)
        if tmp is True:
            upload_path = tempfile.gettempdir()
            _f = open(f'{upload_path}/{filename}{extention}', 'wb')
            _f.write(file_content)
            _f.close()
        else:
            for i in range(0,100):
                filename, extention = os.path.splitext(file.filename)
                if i > 0:
                    filename = secure_filename(f'{filename}({i})')
                else:
                    filename = secure_filename(filename)
                file_exists = os.path.exists(f'{upload_path}/{filename}{extention}')
                if not file_exists:
                    _f = open(f'{upload_path}/{filename}{extention}', 'wb')
                    _f.write(file_content)
                    _f.close()
                    break
                else:
                    continue
        return {
            'success': True,
            'msg': i18n.t('success', file = f'{filename}{extention}'),
            'file': f'{filename}{extention}'
        }
    except Exception as _err:# pylint: disable=broad-exception-caught
        *_, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
        return {
            'success': False,
            'msg': i18n.t('unexpected-error', err = str(_err))
        }
# SAVE LOGS
async def create_log(req, _method, _conf, db, params, res, log): #pylint: disable=unused-argument
    '''CREATE LOGS'''
    try:
        to_ignore = _conf['REQUEST_LOG'].get('ACTIONS_TO_IGNORE')
        # print(to_ignore)
        if _method in to_ignore or not res:
            pass
        else:
            log['res_at'] = datetime.datetime.now()
            log['res_type'] = 'success' if res.get('success') is True else 'error'
            log['res_msg'] = res.get('msg')
            log['row_id'] = res.get('inserted_primary_key') if res else None
            log['table'] =  params['data'].get('table') if params.get('data') else None
            _db = _conf['DATABASE'].get('database') if _conf.get('DATABASE') else None
            _db = params['data'].get('db') if params['data'].get('db') else _db
            log['db'] = params['data'].get('database') if params['data'].get('database') else _db
            log['app_id'] =  params['app'].get('app_id') if params.get('app') else None
            if not _conf['REQUEST_LOG'].get('DO_NOT_STORE_DATA'):
                req_data = dict(req)       
                if params.get('controller') == 'login':
                    if not req_data.get('data'):
                        pass
                    elif req_data['data'].get('password'):
                        del req_data['data']['password']
                log['req_data'] = json.dumps(req_data)
                log['res_data'] = json.dumps(res)
                log['new_data'] = json.dumps(res.get('data'))
                if _conf['REQUEST_LOG'].get('SKIP_SPECIFIC_FIELDS'):
                    try:
                        for field in _conf['REQUEST_LOG'].get('SKIP_SPECIFIC_FIELDS'):
                            if log.get(field):
                                del log[field]
                    except Exception as err_:
                        print(str(err_))
            params['data']['db'] = _conf['DATABASE'].get('database')
            params['app'] = {'app_id': 1, 'app': 'ADMIN', 'db': params['data']['db']}
            params['user'] = {'user_id': 1, 'user': 'root'}
            params['data']['table'] = 'user_log'
            params['data']['data'] = log
            params['database'] = _conf.get('DATABASE')
            crud = Crud(_conf, params, db, i18n.t)
            log_res = await crud.create_update()
            # print('LOGS:', log_res)
    except Exception as _err:# pylint: disable=broad-exception-caught
        *_, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('LOGS DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
# DYNAMIC API
@app.post('/dyn_api/{_class}/{_method}')
@app.post('/dyn_api/{_class}/{_method}/')
async def dyn_api(
    req_all: Request,
    req: Req,
    _class: Union[str, None] = None,
    _method: Union[str, None] = None,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security)
):
    '''THE MAIN DYNAMIC API'''
    try:      
        #https://docs.python.org/3/library/importlib.html
        #https://docs.python.org/3/library/functions.html#getattr
        #https://www.w3schools.com/python/ref_func_getattr.asp
        token = None
        if credentials:
            token = credentials.credentials
        verify = verify_token(token)
        #print(token, verify)
        params = copy.deepcopy(dict(req))
        if params.get('lang'):
            try:
                i18n.set('locale', params.get('lang', 'en'))
            except Exception as _err:# pylint: disable=broad-exception-caught
                print(str(_err))
        #print(_class, _method)
        if not params.get('data'):
            pass
        elif params['data'].get('database') and not params.get('database'):
            params['database'] = params['data'].get('database')
        elif params['data'].get('db') and not params.get('db'):
            params['db'] = params['data'].get('db')
        params['user'] = verify.get('payload')
        db = DB(conf = conf, params = params)
        #print(req_all.client.host)
        log = {
            'user_id': params['user'].get('user_id') if params.get('user') else None,
            'action': f"{_class}/{_method}",
            'req_ip': req_all.client.host,
            'req_at': datetime.datetime.now()
        }
        res = {}
        if not _class:
            res = {'success': False, 'msg': i18n.t('no-class')}
        elif not _method:
            res = {'success': False, 'msg': i18n.t('no-method')}
        elif _class in ['login']:
            login = Login(conf, params, db, i18n.t)
            if _method in ['login', 'index']:
                res = await login.login()
            elif _method in ['alter_pass', 'alterpass', 'alterPass']:
                if verify.get('success') is False:
                    return verify
                res = await login.alter_pass()
            elif _method in ['chk_token', 'chk_session', 'chkToken', 'chkSession', 'verify_token', 'verifyToken']:
                return verify
            else:
                res = {
                    'success': False,
                    'msg': i18n.t('method-not-found', actn = _method)
                }        
        elif _class and _method:
            if not conf.get('DO_NOT_VERIFY_AUTH_TOKEN'):
                if verify.get('success') is False:
                    return verify
            else:
                if f"{_class}/{_method}" in conf['DO_NOT_VERIFY_AUTH_TOKEN']:
                    pass
                elif verify.get('success') is False:
                    return verify
            # from core._class import Class
            # __import__ 
            _my_module = importlib.import_module(f'core.{_class}')
            _my_module_class = getattr(_my_module, f'{_class.capitalize()}')
            _class_instance = _my_module_class(conf, params, db, i18n.t)
            _class_instance_method = getattr(_class_instance, f'{_method}')
            res = await _class_instance_method()
        else:
            res = {
                'success': False,
                'msg': i18n.t('class-not-found', ctrl = _class)
            }
        if conf.get('REQUEST_LOG'):
            if _class == 'login' and not log.get('user_id'):
                log['user_id'] = res['data'].get('user_id') if res.get('data') else None
            await create_log(req, _method, conf, db, params, res, log)
        #print('RES:', type(res))
        if conf.get('BROADCAST_CHANGES') and res.get('success') is True:
            #print('TABLE CHANGES:', res.get('success'), res.get('msg'))
            try:
                if _method.lower() in conf['BROADCAST_CHANGES'].get('ACTIONS'):
                    # print('BROADCAST_CHANGES:', _method.lower())
                    client = TestClient(app)
                    with client.websocket_connect(f"/ws?token={token}") as websocket:
                        p = dict(req)
                        websocket.send_json({
                            "type": "data_change",
                            "database": p['data'].get('db') if p['data'].get('db') else p['app'].get('db'), 
                            "table": p['data'].get('table')
                        })
                        websocket.close()
            except Exception as _err:# pylint: disable=broad-exception-caught
                *_, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print('2: DEBUG WS INF: ', str(_err), fname, exc_tb.tb_lineno)      
        return res
    except Exception as _err:# pylint: disable=broad-exception-caught
        *_, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('DEBUG INF RESPONSE: ', str(_err), fname, exc_tb.tb_lineno)
        return {
            'success': False,
            'msg': i18n.t('unexpected-error', err = str(_err))
        }
# WEBSOKET https://fastapi.tiangolo.com/advanced/websockets/
# https://stackoverflow.com/questions/71827145/broadcasting-a-message-from-a-manager-to-clients-in-websocket-using-fastapi
class ConnectionManager:
    '''Websocket connection manager'''
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        '''connect'''
        await websocket.accept()
        self.active_connections.append(websocket)
        print('active_connections:', len(self.active_connections))
    def disconnect(self, websocket: WebSocket):
        '''disconnect'''
        self.active_connections.remove(websocket)
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        '''send personal message'''
        await websocket.send_json(message)
    async def broadcast(self, message: dict):
        '''broadcast'''
        for connection in self.active_connections:
            await connection.send_json(message)
manager = ConnectionManager()
@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    session: Union[str, None] = Cookie(default=None),
    token: Union[str, None] = Query(default=None)
):
    '''websocket endpoint'''
    try:
        #print(token)
        if not token and session:
            token = session
        if session is None and token is None:
            await websocket.close(code = status.WS_1008_POLICY_VIOLATION)
        else:
            verify = verify_token(token)
            if verify.get('success') is True:
                await manager.connect(websocket)
                while True:
                    data = await websocket.receive_json()
                    await manager.broadcast(data)
            else:
                await websocket.close(code = status.WS_1008_POLICY_VIOLATION)
    except WebSocketDisconnect:
        try:
            manager.disconnect(websocket)
            await websocket.close()
        except Exception as _err:# pylint: disable=broad-exception-caught
            pass
#uvicorn main:app --reload --port 8080 --workers 4
if __name__ == '__main__':
    #print('CONFIG:', conf)
    try:
        LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
        uvicorn.run('main:app', host = '0.0.0.0', port = conf.get('PORT'), log_level = 'info', reload = True)
    except Exception as _err:# pylint: disable=broad-exception-caught
        print(str(_err))
