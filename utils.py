from datetime import datetime as dt, date
from pybeans import AppTool

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import os
ENV = os.environ.get('ENV', 'dev')

import re
REG_DATE = re.compile(r'(\d{4})\D*(\d{1,2})?\D*(\d{1,2})?')

class FinUtil(AppTool):
    """
    蜘蛛公用代码
    """
    def __init__(self, spider_name):
        super(FinUtil, self).__init__(spider_name)
        self._session = None
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            assert(self['db'] is not None)
            db = self['db']
            self.debug(f'连接数据库：{db['db']}')
            self._engine = create_engine(f"postgresql+psycopg://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['db']}", echo=False)
        return self._engine

    @property
    def session(self):
        """
        Lazy loading
        """
        if self._session:
            return self._session
        self._session = sessionmaker(bind=self.engine)
        return self._session
    
        
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
        
        
today = date.today()
fin = FinUtil('fin')