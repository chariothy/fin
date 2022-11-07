import requests
import json
import time
import hmac
import base64
import hashlib
import urllib.parse


def create_sign_for_dingtalk(secret: str):
    """
    docstring
    """
    timestamp = str(round(time.time() * 1000))
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return timestamp, sign


DINGTALK_API_URL="https://oapi.dingtalk.com/robot/send?access_token={}"
DINGTAIL_HEADERS = {'Content-Type': 'application/json'}

def do_notify_by_ding_talk(dingtalk_config: dict, data: dict):
    """发消息给钉钉机器人
    """
    token = dingtalk_config['token']
    secret = dingtalk_config['secret']
    
    assert token and secret

    url = DINGTALK_API_URL.format(token)
    timestamp, sign = create_sign_for_dingtalk(secret)
    url += f'&timestamp={timestamp}&sign={sign}'
    
    #APP.debug(f'钉钉机器人 数据===> {data}')
    return requests.post(url=url, headers = DINGTAIL_HEADERS, data=json.dumps(data))


# for examples:
DINGTAIL_SUBJECT = "[GITHOOK] {pusher}推送项目{rep_name}{result}"
DINGTAIL_BODY = """## {pusher}推送项目[{rep_name}]({url}){result}\n
### <font color=red>COMMITS：</font>\n
{comment_li}\n
### <font color=red>COMMANDS：</font>\n
{command_li}\n
### <font color=red>STDOUT：</font>\n
{stdout_li}\n
### <font color=red>STDERR：</font>\n
{stderr_li}
"""

#dt_data = data.copy()
#dt_data['comment_li'] = '\n'.join((f'- {c}' for c in data['comments']))
#dt_data['command_li'] = '\n'.join((f'- {c}' for c in data['commands']))
#dt_data['stdout_li'] = '\n'.join((f'- {c}' for c in data['stdout_list']))
#dt_data['stderr_li'] = '\n'.join((f'- {c}' for c in data['stderr_list']))

def notify_by_ding_talk(dingtalk_config: dict, title: str, text: str):
    """发消息给钉钉机器人
    """
    dt_msg = {
        "msgtype": 'markdown',
        "markdown": {
            'title': title,
            'text': text
        }
    }
    res = do_notify_by_ding_talk(dingtalk_config, dt_msg)
    return res.json()