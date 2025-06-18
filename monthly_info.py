import datetime, json
from sqlalchemy import orm, Column, String, DATE, select
import pandas as pd
from macro import Macro
from utils import fin

now = datetime.datetime.now()
ten_years_ago = now.replace(year=now.year - 10)
base_date = ten_years_ago.strftime('%Y-%m-01')

macro_dict = {}
items = [None, None, None, None, None, None, None, None] #, None, None, None]
CPI = 0
PPI = 1
PMI = 2
M1 = 3
M2 = 4
M1M2 = 5
RETAIL = 6
FINANCING = 7
C_LEVERR = 8
E_LEVERR = 9
G_LEVERR = 10

def _get_data(name):
    stmt = select(Macro).where(Macro.name == name)
    with fin.session() as sess:
        # 执行查询
        result = sess.scalars(stmt).first()
        return result.data
            
def update_monthly(filepath):
    json_data = _get_data('CPI')
    for item in json_data:
        date = item[0]
        current = item[1]
        if date > base_date and current is not None:
            new_date = date[:-3]  # 去除日
            if new_date not in macro_dict:
                macro_dict[new_date] = items.copy()
            macro_dict[new_date][CPI] = current
    
    json_data = _get_data('PPI')
    for item in json_data:
        date = item[0]
        current = item[1]
        if date > base_date and current is not None:
            new_date = date[:-3]  # 去除日
            if new_date not in macro_dict:
                macro_dict[new_date] = items.copy()
            macro_dict[new_date][PPI] = current
    
    json_data = _get_data('PMI')
    for item in json_data:
        date = item[0]
        current = item[1]
        if date > base_date and current is not None:
            new_date = date[:-3]  # 去除日
            if new_date not in macro_dict:
                macro_dict[new_date] = items.copy()
            macro_dict[new_date][PMI] = current

    json_data = _get_data('MONEY')
    for item in json_data:
        date = item[0]
        m1 = item[1]
        m2 = item[2]
        m1m2 = item[3]
        if date > base_date:
            if date not in macro_dict:
                macro_dict[date] = items.copy()
            macro_dict[date][M1] = m1
            macro_dict[date][M2] = m2
            macro_dict[date][M1M2] = m1m2
    
    json_data = _get_data('RETAIL')
    for item in json_data:
        date = item[0]
        retail = item[1]
        if date > base_date:
            if date not in macro_dict:
                macro_dict[date] = items.copy()
            macro_dict[date][RETAIL] = retail

    json_data = _get_data('FINANCING')
    for item in json_data:
        date = item[0]
        financing = item[2]
        if date > base_date:
            if date not in macro_dict:
                macro_dict[date] = items.copy()
            macro_dict[date][FINANCING] = financing
    
    # json_data = _get_data('LEVERR')
    # for item in json_data:
    #     date = item[0]
    #     c_level = item[1] # citizen
    #     e_level = item[2] # non-financial enterprise
    #     g_level = item[3] # government
    #     if date > base_date:
    #         if date not in macro_dict:
    #             macro_dict[date] = items.copy()
    #         macro_dict[date][C_LEVERR] = c_level
    #         macro_dict[date][E_LEVERR] = e_level
    #         macro_dict[date][G_LEVERR] = g_level
    
    df = pd.DataFrame([(k, *v) for k, v in macro_dict.items()], columns=['年月', 'CPI', 'PPI', 'PMI', 'M1同比', 'M2同比', 'M1-M2同比', '社零同比', '社融同比']) #, '居民杠杆', '非银杠杆', '政府杠杆'])
    df = df.sort_values('年月')
    print(df)
    
    
    json_data = _get_data('931995')
    sorted_data = sorted(json_data, key=lambda x: x['date'])
    
    latest_date_str = sorted_data[-1]['date']
    latest_date = datetime.datetime.strptime(latest_date_str, '%Y-%m-%d')
    today = datetime.datetime.today()
    date_diff = abs((latest_date - today).days)

    if date_diff <= 5:
        fin.error("> 最新海通资产配置日期距离今天不超过5天：")
        fin.info(sorted_data[-1])
    
    with pd.ExcelWriter(filepath, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name='宏观', index=False, header=True, startrow=0, startcol=0)
    
if __name__ == '__main__':
    update_monthly(r'C:\Users\Henry\OneDrive\henry\投资\资配系统-20250215.xlsx')