'''mail'''
# pylint: disable=unused-variable
# pylint: disable=line-too-long
# pylint: disable=wrong-import-order
# pylint: disable=broad-exception-caught
# pylint: disable=unused-import
# pylint: disable=invalid-name
# pylint: disable=trailing-whitespace
# pylint: disable=broad-exception-caught
import tempfile
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email import encoders
import email
import email.mime.application
import datetime
from icalendar import Calendar, Event
import os
import sys
from dateutil import parser
try:
    import win32com.client
except Exception as _err:
    win32com = None

class Mail:
    '''mail'''
    def __init__(self, conf, params, db, i18n):
        self.conf = conf
        self.params = params
        self.db = db
        self.i18n = i18n

    def get_event(self, _data): #https://icalendar.org/
        '''get event'''
        event = Event()
        event['dtstart'] = _data['dtstart']
        if isinstance(_data['dtstart'], datetime.datetime):
            event['dtstart'] = _data['dtstart']#.strftime("%Y%m%dT%H%M%SZ")
        else:
            event['dtstart'] = parser.parse(event['dtstart'])
        event['dtend'] = _data['dtend']
        if isinstance(_data['dtend'], datetime.datetime):
            event['dtend'] = _data['dtend']#.strftime("%Y%m%dT%H%M%SZ")
        else:
            event['dtend'] = parser.parse(event['dtend'])
        event['summary'] = _data['summary']
        event['attendee'] = _data.get('attendee', [])
        if _data.get('uid'):
            event.add('uid', _data.get('uid'))
        if _data.get('dtstamp'):
            if isinstance(_data['dtstamp'], datetime.datetime):
                event.add('dtstamp', _data.get('dtstamp'))#.strftime("%Y%m%dT%H%M%SZ"))
            else:
                event.add('dtstamp', parser.parse(_data.get('dtstamp')))
        if _data.get('organizer'):
            event.add('organizer', _data.get('organizer'))
        if _data.get('priority'):
            event.add('priority', _data.get('priority'))
        if _data.get('location'):
            event.add('location', _data.get('location'))
        if _data.get('geo'):
            event.add('geo', _data.get('geo')) # 37.5739497;-85.7399606
        if _data.get('description'):
            event.add('description', _data.get('description'))
        if _data.get('rrule'):
            event.add('rrule', _data.get('rrule')) # FREQ=YEARLY;INTERVAL=1;BYMONTH=2;BYMONTHDAY=12
        return event
    def get_icall(self, _data):
        '''get icall'''
        cal = Calendar()
        cal.add('version', '2.0')
        if _data.get('event'):
            cal.add_component(self.get_event(_data.get('event')))
        else:
            cal['dtstart'] = _data['dtstart']
            if isinstance(_data['dtstart'], datetime.datetime):
                cal['dtstart'] = _data['dtstart']#.strftime("%Y%m%dT%H%M%SZ")
            else:
                cal['dtstart'] = parser.parse(_data['dtstart'])
            cal['dtend'] = _data['dtend']
            if isinstance(_data['dtend'], datetime.datetime):
                cal['dtend'] = _data['dtend']#.strftime("%Y%m%dT%H%M%SZ")
            else:
                cal['dtend'] = parser.parse(_data['dtend'])
            cal['summary'] = _data['summary']
            cal['attendee'] = _data['attendee']
            if _data.get('uid'):
                cal.add('uid', _data.get('uid'))
            if _data.get('priority'):
                cal.add('priority', _data.get('priority'))
            if _data.get('location'):
                cal.add('location', _data.get('location'))
            if _data.get('geo'):
                cal.add('geo', _data.get('geo'))# 37.5739497;-85.7399606
            if _data.get('description'):
                cal.add('description', _data.get('description'))
            if _data.get('rrule'):
                cal.add('rrule', _data.get('rrule'))# FREQ=YEARLY;INTERVAL=1;BYMONTH=2;BYMONTHDAY=12 https://icalendar.org/rrule-tool.html
        return cal
    async def event(self):
        'smtp send event'
        try:
            if not self.params.get('mail') and self.params['data'].get('mail'):
                self.params['mail'] = self.params['data'].get('mail')
            sender_email  = self.params['mail'].get('from') if self.params['mail'].get('from') else os.environ.get('EMAIL')
            smtp_server = self.params['mail'].get('smtp') if self.params['mail'].get('smtp') else os.environ.get('SMTP_SERVER')
            port = self.params['mail'].get('port') if self.params['mail'].get('port') else os.environ.get('SMTP_PORT')
            password = self.params['mail'].get('pass') if self.params['mail'].get('pass') else os.environ.get('EMAIL_PASSWORD')
            message = MIMEMultipart('mixed')
            message['From']    = sender_email #self.params['mail'].get('from', os.environ.get('EMAIL'))
            message['To']      = self.params['mail']['to']
            message['Subject'] = self.params['mail']['subject']
            context = ssl.create_default_context() 
            part_email = MIMEText(self.params['mail']['body'], self.params['mail'].get('type', 'html'))
            cal = self.get_icall(self.params.get('mail'))
            ical = cal.to_ical().decode('utf8')
            print('ical:', ical)
            part_cal = MIMEText(ical,'calendar;method=REQUEST')
            msg_alternative = MIMEMultipart('alternative')
            message.attach(msg_alternative)
            ical_atch = MIMEBase('application/ics',' ;name="invite.ics"')
            ical_atch.set_payload(ical)
            encoders.encode_base64(ical_atch)
            ical_atch.add_header('Content-Disposition', 'attachment; filename="invite.ics"')
            eml_atch = MIMEBase('text/plain','')
            #encoders.encode_base64(eml_atch)
            eml_atch.add_header('Content-Transfer-Encoding', "")
            msg_alternative.attach(part_email)
            msg_alternative.attach(part_cal)
            with smtplib.SMTP(smtp_server, port) as server:
                #server.set_debuglevel(1)
                server.ehlo()
                server.starttls(context = context) # Secure the connection
                server.ehlo()
                server.login(sender_email, password)
                server.sendmail(
                    sender_email,
                    self.params['mail']['to'], 
                    message.as_string()
                )
                return  {'success': True, 'msg': self.i18n('success')}
        except Exception as _err:
            try:
                server.quit()
            except Exception as _err2:
                pass
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
    async def smtp(self):
        'smtp send mail'
        if not self.params.get('mail') and self.params['data'].get('mail'):
            self.params['mail'] = self.params['data'].get('mail')
        return await self.smtp_send()    
    async def send(self):
        'smtp send mail'
        if not self.params.get('mail') and self.params['data'].get('mail'):
            self.params['mail'] = self.params['data'].get('mail')
        return await self.smtp_send()
    async def smtp_send(self):
        'smtp send mail'
        try:
            sender_email  = self.params['mail'].get('from') if self.params['mail'].get('from') else os.environ.get('EMAIL')
            smtp_server = self.params['mail'].get('smtp') if self.params['mail'].get('smtp') else os.environ.get('SMTP_SERVER')
            port = self.params['mail'].get('port') if self.params['mail'].get('port') else os.environ.get('SMTP_PORT')
            password = self.params['mail'].get('pass') if self.params['mail'].get('pass') else os.environ.get('EMAIL_PASSWORD')
            #print(sender_email, smtp_server, port, password)
            message = MIMEMultipart('mixed')
            message['From']    = sender_email #self.params['mail'].get('from', os.environ.get('EMAIL'))
            message['To']      = self.params['mail']['to']
            message['Subject'] = self.params['mail']['subject']
            if self.params['mail'].get('event'):
                # print('event:'.upper(), self.params['mail'].get('event'))
                cal = self.get_icall(self.params.get('mail'))
                self.params['mail']['body'] = cal.to_ical().decode('utf8')
                #self.params['mail']['type'] = 'calendar;method=REQUEST'
                msg_alternative = MIMEMultipart('alternative')
                ical_atch = MIMEBase('text/calendar',' ;name="invitation.ics"')
                ical_atch.set_payload(self.params['mail']['body'])
                encoders.encode_base64(ical_atch)
                ical_atch.add_header('Content-Disposition', 'attachment; filename="invitation.ics"')
                print(cal.to_ical().decode('utf8'))
                msg_alternative.attach(MIMEText(cal.to_ical().decode('utf8'),'calendar;method=REQUEST'))
                msg_alternative.attach(ical_atch)
                message.attach(msg_alternative)
            if not self.params['mail'].get('attachments'):
                pass
            elif len(self.params['mail'].get('attachments')) > 0:
                for att in self.params['mail'].get('attachments'):
                    try:
                        path = f'{os.getcwd()}/{self.conf.get("UPLOAD")}/{att}'
                        _, fname = os.path.split(att)
                        _, ext = os.path.splitext(att)
                        _file = open(path,'rb')
                        att = email.mime.application.MIMEApplication(_file.read(), _subtype = ext)
                        _file.close()
                        att.add_header('Content-Disposition','attachment',filename = fname)
                        message.attach(att)
                    except Exception as _mail_att_err:
                        print(att, str(_mail_att_err))
            message.attach(MIMEText(self.params['mail']['body'], self.params['mail'].get('type', 'html')))
            context = ssl.create_default_context()
            with smtplib.SMTP(smtp_server, port) as server:
                #server.set_debuglevel(1)
                server.ehlo()
                server.starttls(context = context) # Secure the connection
                server.ehlo()
                server.login(sender_email, password)
                server.sendmail(
                    sender_email,
                    self.params['mail']['to'], 
                    message.as_string()
                )
                return  {'success': True, 'msg': self.i18n('success')}              
        except Exception as _err:
            try:
                server.quit()
            except Exception as _err2:
                pass
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
    async def outlook(self):
        'outlook send mail'
        try:
            if not self.params.get('mail') and self.params['data'].get('mail'):
                self.params['mail'] = self.params['data'].get('mail')
            _mail = self.params['mail']
            # {to: [], cc: [], subject: '', body: '', attachments: [], tmp: true, tmp2: false, full_path: null, att_str: null}
            obj = win32com.client.Dispatch("Outlook.Application")
            outlook = obj.CreateItem(0)
            outlook.To = ';'.join(_mail.get('to')) if isinstance(_mail.get('to'), list) else _mail.get('to')
            outlook.CC = ';'.join(_mail.get('cc')) if isinstance(_mail.get('cc'), list) else _mail.get('cc')
            outlook.Subject = _mail.get('subject')
            outlook.Body = _mail.get('body')
            _path = f'{os.getcwd()}/{self.conf.get("UPLOAD")}'
            if _mail.get('tmp2') is True:
                _path = tempfile.gettempdir()
            elif _mail.get('tmp') is True:
                _path = f'{_path}/tmp'
            if not _mail.get('attachments'):
                pass
            elif isinstance(_mail.get('attachments'), list):
                for att in _mail.get('attachments'):
                    outlook.Attachments.Add(f'{_path}/{att}', Type = 1, DisplayName = att)
            else:
                if not os.path.exists(f'{_path}/{_mail.get("attachments")}') and _mail.get('fname') and _mail.get('att_str'):
                    fname = _mail.get('fname')
                    _file = open(f'{_path}/{fname}', mode = 'w', encoding = 'utf-8')
                    _file.write(_mail.get('attachments'))
                    _file.close()
                    outlook.Attachments.Add(f'{_path}/{fname}', Type = 1, DisplayName = _mail.get('attachments'))
                else:       
                    outlook.Attachments.Add(f'{_path}/{_mail.get("attachments")}', Type = 1, DisplayName = _mail.get('attachments'))
            outlook.Send()
            return  {'success': True, 'msg': self.i18n('success')}
        except Exception as _err:
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF OUTLLOK: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
    async def outlook_invite(self):
        'outlook send invite'
        try:
            if not self.params.get('mail') and self.params['data'].get('mail'):
                self.params['mail'] = self.params['data'].get('mail')
            _mail = self.params['mail']
            obj = win32com.client.Dispatch("Outlook.Application")
            outlook = obj.CreateItem(1)   # 1 represents olAppointmentItem (Outlook Appointment)
            outlook.Subject = _mail.get('subject')
            outlook.Body = _mail.get('body')
            outlook.MeetingStatus = 1  # 1 represents olMeeting (Outlook Meeting)
            #outlook.Import(f'{os.getcwd()}/invite.ics', 2)  # 2 represents olICal (Outlook iCalendar Appointment)
            # Add attendees
            for attendee in _mail.get('to'):
                outlook.Recipients.Add(attendee)
            _path = f'{os.getcwd()}/{self.conf.get("UPLOAD")}'
            if _mail.get('tmp2') is True:
                _path = tempfile.gettempdir()
            elif _mail.get('tmp') is True:
                _path = f'{_path}/tmp'
            if not _mail.get('attachments'):
                pass
            elif isinstance(_mail.get('attachments'), list):
                for att in _mail.get('attachments'):
                    outlook.Attachments.Add(f'{_path}/{att}', Type = 1, DisplayName = att)
            else:
                if not os.path.exists(f'{_path}/{_mail.get("attachments")}') and _mail.get('fname') and _mail.get('att_str'):
                    fname = _mail.get('fname')
                    _file = open(f'{_path}/{fname}', mode = 'w', encoding = 'utf-8')
                    _file.write(_mail.get('attachments'))
                    _file.close()
                    outlook.Attachments.Add(f'{_path}/{fname}', Type = 1, DisplayName = _mail.get('attachments'))
                else:       
                    outlook.Attachments.Add(f'{_path}/{_mail.get("attachments")}', Type = 1, DisplayName = _mail.get('attachments'))
            outlook.Send()
            return  {'success': True, 'msg': self.i18n('success')}
        except Exception as _err:
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('DEBUG INF OUTLLOK: ', str(_err), fname, exc_tb.tb_lineno)
            return {'success': False, 'msg': self.i18n('unexpected-error', err = str(_err))}
            
    