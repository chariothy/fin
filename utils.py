import os
from datetime import datetime as dt
from pybeans import AppTool
import json
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from notify import notify_by_ding_talk

import re
REG_DATE = re.compile(r'(\d{4})\D*(\d{1,2})?\D*(\d{1,2})?')

class FinUtil(AppTool):
    """
    蜘蛛公用代码
    """
    def __init__(self, spider_name):
        super(FinUtil, self).__init__(spider_name)
        self._session = None


    @property
    def session(self):
        """
        Lazy loading
        """
        if self._session:
            return self._session
        assert(self['mysql'] is not None)
        DB_CONN = 'mysql+mysqlconnector://{c[user]}:{c[pwd]}@{c[host]}:{c[port]}/{c[db]}?ssl_disabled=True' \
            .format(c=self['mysql'])
        engine = create_engine(
            DB_CONN, 
            pool_size=20, 
            max_overflow=0, 
            echo=self['log.sql'] == 1,
            json_serializer=lambda obj: json.dumps(obj, ensure_ascii=False))
        self._session = sessionmaker(bind=engine)()
        return self._session


    def ding(self, title: str, text: str):
        result = notify_by_ding_talk(self['dingtalk'], title, text)
        self.D(result)
        
        
def parse_date(mode: str, date_str: str) -> str:
    """日期解析

    Args:
        mode (str): 模式：Y / YM / YMD
        date_str (str): 包含日期的字符串

    Returns:
        str: YYYY-MM-DD
    """
    m = REG_DATE.match(date_str)
    assert m
    g = m.groups()
    if mode == 'Y':
        return g[0]
    elif mode == 'YM' and len(g) > 1:
        return g[0] + '-' + g[1].rjust(2, '0')
    elif mode == 'YMD' and len(g) > 2:
        return g[0] + '-' + g[1].rjust(2, '0') + '-' + g[2].rjust(2, '0')
    
        
def parse_y(date_str: str) -> str:
    """日期解析

    Args:
        date_str (str): 包含日期的字符串

    Returns:
        str: YYYY
    """
    return parse_date('Y', date_str)

        
def parse_ym(date_str: str) -> str:
    """日期解析

    Args:
        date_str (str): 包含日期的字符串

    Returns:
        str: YYYY-MM
    """
    return parse_date('YM', date_str)


def parse_ymd(date_str: str) -> str:
    """日期解析

    Args:
        date_str (str): 包含日期的字符串

    Returns:
        str: YYYY-MM-DD
    """
    return parse_date('YMD', date_str)
        
        
fu = FinUtil('fin')