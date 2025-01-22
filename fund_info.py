import akshare as ak
import openpyxl
import pandas as pd
from utils import fin

CODE = 0
NAME = 1
ALL_RETURN = 10


def set_value(ws_cell, df_cell):
    if not pd.isna(df_cell):
        ws_cell.value = df_cell


def get_fund_info():
    off_exchg_df = ak.fund_open_fund_rank_em(symbol="全部")
    fin.info(f'获取到场外基金数据{len(off_exchg_df)}条')
    on_exchg_df = ak.fund_exchange_rank_em()
    fin.info(f'获取到场内基金数据{len(on_exchg_df)}条')
    #print(df)
        
    wb = openpyxl.load_workbook(fin['asset_config_path'])
    try:
        sheet = wb['基金']
        data = []
        for row in sheet.iter_rows(min_row=2):
            #print(row)
            code = row[CODE].value
            name = row[NAME].value
            all_return_cell = row[ALL_RETURN]
            
            result = off_exchg_df[off_exchg_df['基金代码'] == code]
            fund_name = ''
            if not result.empty:
                fund_name = result.iloc[0]['基金简称']
            else:
                result = on_exchg_df[on_exchg_df['基金代码'] == code]
                if not result.empty:
                    fund_name = result.iloc[0]['基金简称']
                else:
                    fin.error(f"基金代码：{code}，未找到")
                    continue
                    
            if fund_name:
                set_value(all_return_cell, result.iloc[0]['成立来'])
                set_value(all_return_cell.offset(column=1), result.iloc[0]['近1周'])
                set_value(all_return_cell.offset(column=2), result.iloc[0]['近1月'])
                set_value(all_return_cell.offset(column=3), result.iloc[0]['近3月'])
                set_value(all_return_cell.offset(column=4), result.iloc[0]['近6月'])
                set_value(all_return_cell.offset(column=5), result.iloc[0]['近1年'])
                set_value(all_return_cell.offset(column=6), result.iloc[0]['近2年'])
                set_value(all_return_cell.offset(column=7), result.iloc[0]['近3年'])
                set_value(all_return_cell.offset(column=8), result.iloc[0]['今年来'])
        
        wb.save(fin['asset_config_path'])
    finally:
        wb.close()

if __name__ == "__main__":
    get_fund_info()