import datetime, json
from sqlalchemy import orm, Column, String, DATE, select
import pandas as pd
from macro import Macro
from utils import fin

now = datetime.datetime.now()
five_years_ago = now.replace(year=now.year - 5)
base_date = five_years_ago.strftime('%Y-%m-%d')

macro_dict = {}
items = [None, None, None, None, None, None, None]
BOND10_CN = 0
BOND10_US = 1
SHIBOR = 2
FINANCING_BALANCE = 3
MARGIN_BALANCE = 4
SENTIMENT = 5
HS300 = 6


def _get_data(name):
    stmt = select(Macro).where(Macro.name == name)
    with fin.ro_session() as sess:
        # 执行查询
        result = sess.scalars(stmt).first()
        return result.data
            
def update_daily(filepath):
    json_data = _get_data('BOND10')
    for item in json_data:
        date = item[0]
        cn = item[1]
        us = item[2]
        if date > base_date:
            if date not in macro_dict:
                macro_dict[date] = items.copy()
            macro_dict[date][BOND10_CN] = cn
            macro_dict[date][BOND10_US] = us
    
    json_data = _get_data('SHIBOR')
    for item in json_data:
        date = item[0]
        interest = item[1]
        if date > base_date:
            if date not in macro_dict:
                macro_dict[date] = items.copy()
            macro_dict[date][SHIBOR] = interest
    
    json_data = _get_data('MARGIN')
    for item in json_data:
        date = item[0]
        f_balance = item[1]
        m_balance = item[2]
        if date > base_date:
            if date not in macro_dict:
                macro_dict[date] = items.copy()
            macro_dict[date][FINANCING_BALANCE] = f_balance
            macro_dict[date][MARGIN_BALANCE] = m_balance
    
    json_data = _get_data('SENTIMENT')
    for item in json_data:
        date = item[0]
        sentiment = item[1]
        hs300 = item[2]
        if date > base_date:
            if date not in macro_dict:
                macro_dict[date] = items.copy()
            macro_dict[date][SENTIMENT] = sentiment
            macro_dict[date][HS300] = hs300

    df = pd.DataFrame([(k, *v) for k, v in macro_dict.items()], columns=['年月日', '中国10债', '美国10债', '同业拆借', '融资余额', '融券余额', '市场情绪', '沪深300点位'])
    df = df.sort_values('年月日')
    print(df)
    with pd.ExcelWriter(filepath, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name='高频', index=False, header=True, startrow=0, startcol=0)
    
    
if __name__ == '__main__':
    update_daily(r'C:\Users\Henry\OneDrive\henry\投资\资配系统-20250215.xlsx')