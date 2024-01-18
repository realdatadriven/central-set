'''login'''
import os
import sys
import datetime
import re
from sqlalchemy import Table
from sqlalchemy.sql import select
from sqlalchemy.sql import update
from sqlalchemy.sql import or_
from jose import jwt
#from passlib.hash import pbkdf2_sha256
from passlib.context import CryptContext

#import hashlib

pwd_context = CryptContext(schemes = ["bcrypt"], deprecated = "auto")

class Login:
    '''LOGIN'''
    def __init__(self, conf, params, db, i18n):
        self.conf = conf
        self.params = params
        self.db = db
        self.i18n = i18n
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        '''verify password'''
        return pwd_context.verify(plain_password, hashed_password)
    def get_password_hash(self, password: str) -> str:
        '''get password hash'''
        return pwd_context.hash(password)
    async def login(self):
        '''login'''
        if not self.params.get('data'):
            return {
                'success': False,
                'msg': self.i18n('no-data-recivied')
            }
        elif not self.params['data'].get('username'):
            return {
                'success': False,
                'msg': self.i18n('no-user-recivied')
            }
        elif not self.params['data'].get('password'):
            return {
                'success': False,
                'msg': self.i18n('no-pass-recivied')
            }
        else:     
            try:
                engine = self.db.get_engine()
                metadata = self.db.get_metadata(engine)
                user = Table('user', metadata, autoload_with = engine)
                username = str(self.params['data']['username'])
                # https://docs.sqlalchemy.org/en/20/tutorial/data_select.html
                query = select(user.c).\
                        select_from(user).\
                        where(or_(user.c.username == username, user.c.email == username))
                # print(query)
                with engine.connect() as conn:
                    results = conn.execute(query)
                    res = results.mappings().all()
                    results.close()
                    conn.close()
                    engine.dispose()
                    data = []
                    for row in res:
                        _d = {}
                        for column in dict(row):
                            _d[column] = row[column]
                            if isinstance(_d[column], (datetime.datetime, datetime.date, datetime.time)):
                                _d[column] = _d[column].isoformat()
                        data.append(_d)
                    if len(data) == 0:
                        return {
                            'success': False,
                            'msg': self.i18n('user-pass-incorrect')
                        }
                    elif len(data) > 1:
                        return {
                            'success': False,
                            'msg': self.i18n('user-conflict')
                        }
                    elif len(data) == 1:
                        password = str(self.params['data']['password'])
                        user_data = data[0]
                        # print(user_data)
                        pass_verified = self.verify_password(password, user_data['password'])
                        if not user_data['active']:
                            return {
                                'success': False,
                                'msg': self.i18n('user-deactivated', user = user_data['username'])
                            }
                        elif user_data['excluded']:
                            return {                            
                                'success': False,
                                'msg': self.i18n('user-excluded', user = user_data['username'])
                            }
                        elif pass_verified:
                            user_data['exp'] = datetime.datetime.utcnow() + datetime.timedelta(
                                minutes = self.conf.get('ACCESS_TOKEN_EXPIRE_MINUTES')
                            ) # Expiration -> int -> The instime after which the token is invalid.
                            user_data['iat'] = datetime.datetime.utcnow() # Issued At -> int -> The time at which the JWT was issued.
                            user_data['nbf'] = datetime.datetime.utcnow() # Not Before -> int -> The time before which the token is invalid.                          
                            token = jwt.encode(user_data, self.conf.get('SECRET_KEY'), algorithm = self.conf.get('ALGORITHM'))
                            #print(token)
                            try:
                                user_data['exp'] = user_data['exp'].isoformat()
                                user_data['iat'] = user_data['iat'].isoformat()
                                user_data['nbf'] = user_data['nbf'].isoformat()
                                engine.dispose()
                            except Exception as _err:# pylint: disable=broad-exception-caught
                                print('err:', _err)
                            return {
                                'success': True,
                                'msg': self.i18n('login-success'),
                                'data': user_data,
                                'token': str(token)
                            }
                        else:
                            return {
                                'success': False,
                                'msg': self.i18n('user-pass-incorrect')
                            }
                    else:                  
                        try:
                            conn.close()
                            engine.dispose()
                        except Exception as _err:# pylint: disable=broad-exception-caught
                            print(_err)
                        return {
                            'success': False,
                            'msg': self.i18n('user-pass-incorrect')
                        }
            except Exception as _err:# pylint: disable=broad-exception-caught
                *_, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)        
                try:
                    engine.dispose()
                except Exception as _err2: # pylint: disable=broad-exception-caught
                    print(_err2)
                return {
                    'success': False,
                    'msg': self.i18n('unexpected-error', err = str(_err))
                }
    async def alter_pass(self):
        '''alter password'''
        try:
            login = await self.login()
            if login['success'] is True:
                _data = self.params['data']
                if not _data.get('new_password'):
                    return {
                        'success': False,
                        'msg': self.i18n('new_pass_is_required')
                    }
                elif _data.get('new_password') == _data.get('password'):
                    return {
                        'success': False,
                        'msg': self.i18n('new_pass_old_pass')
                    }
                elif len(_data.get('new_password')) < 8:
                    return {
                        'success': False,
                        'msg': self.i18n('password_min_length')
                    }
                elif not len(re.findall(r'[A-Z]', _data.get('new_password'))) > 0:
                    return {
                        'success': False,
                        'msg': self.i18n('pass_must_have_upper')
                    }
                elif not len(re.findall(r'[0-9]', _data.get('new_password'))) > 0:
                    return {
                        'success': False,
                        'msg': self.i18n('pass_must_have_number')
                    }
                elif not len(re.findall(r'[$&+,:;=?@#]', _data.get('new_password'))) > 0:
                    return {
                        'success': False,
                        'msg': self.i18n('pass_must_have_special')
                    }
                engine = self.db.get_engine()
                metadata = self.db.get_metadata(engine)
                user = Table('user', metadata, autoload_with = engine)
                password = self.get_password_hash(str(self.params['data']['new_password']))
                query = update(user).\
                        where(
                            or_(
                                user.c.username == self.params['data']['username'],
                                user.c.email == self.params['data']['username']
                            )
                        ).values(password = password)
                        #password = hashlib.md5(str(self.params['data']['newPassword']).encode('utf-8')).hexdigest()
                        #password = pbkdf2_sha256.hash(str(self.params['data']['password']))
                try:
                    with engine.connect() as conn:
                        result = conn.execute(query)
                        result.close()
                        conn.close()
                        engine.dispose()
                        return {
                            'success': True,
                            'msg': self.i18n('alter-pass-success')
                        }
                except Exception as _err:# pylint: disable=broad-exception-caught
                    try:
                        conn.close()
                        engine.dispose()
                    except Exception as _err2: # pylint: disable=broad-exception-caught
                        print(_err2)
                    return {
                        'success': False,
                        'msg': self.i18n('unexpected-error', err = str(_err))
                    }
            else:
                return login
        except Exception as _err:# pylint: disable=broad-exception-caught
            return {
                'success': False,
                'msg': self.i18n('unexpected-error', err = str(_err))
            }