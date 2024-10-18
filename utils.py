from datetime import datetime as dt
from pybeans import AppTool
import datetime

from sqlalchemy import create_engine, text

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


    def ding(self, title: str, text: str):
        result = notify_by_ding_talk(self['dingtalk'], title, text)
        self.D(result)
        

    def dict_to_where(self, wheres: tuple, row) -> str:
        clauses = []
        for key in wheres:
            value = row[key]
            if isinstance(value, str):
                clauses.append(f"{key} = '{value}'")
            elif isinstance(value, (int, float)):
                clauses.append(f"{key} = {value}")
            elif isinstance(value, datetime.date):
                clauses.append(f"{key} = '{value.strftime('%Y-%m-%d')}'")
            else:
                raise ValueError(f"Unsupported data type for key: {key}, value: {value}")
        return " AND ".join(clauses)
        
        
    def save(self, df, table, wheres, dtype=None):
        db = self['db']
        engine = create_engine(f"postgresql+psycopg://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['db']}")
        # 将数据写入数据库
        inserted = 0
        with engine.connect() as connection:
            self.debug(connection)
            for _, row in df.iterrows():
                where = self.dict_to_where(wheres, row)
                # 查询是否已经存在相同的code和date组合
                query = f"SELECT COUNT(*) FROM {table} WHERE {where}"
                result = connection.execute(text(query)) ## Must use text() for psycopg3
                count = result.scalar()

                if count == 0:
                    try:
                        # 将数据写入数据库
                        row.to_frame().T.to_sql(table, engine, if_exists='append', index=False, dtype=dtype)
                        inserted += 1
                        self.debug(f"Data for {where} inserted successfully.")
                    except Exception as e:
                        self.fatal(f"Error inserting data for {where} into the database:")
        return inserted

        
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
        
        
fin = FinUtil('fin')