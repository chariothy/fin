import akshare as ak
import openpyxl
import pandas as pd
from utils import fin

CODE = 0
NAME = 1
FOUND_DATE = 3
SCALE = 5
SALE = 6
FEE = 7
MANAGER = 8
ALL_RETURN = 11


def set_value(ws_cell, df_cell):
    if not pd.isna(df_cell):
        ws_cell.value = df_cell / 100


def get_fund_info():
    off_exchg_df = ak.fund_open_fund_rank_em(symbol="全部")
    fin.info(f'获取到场外基金数据{len(off_exchg_df)}条')
    on_exchg_df = ak.fund_exchange_rank_em()
    fin.info(f'获取到场内基金数据{len(on_exchg_df)}条')
    money_df = ak.fund_money_rank_em()
    fin.info(f'获取到货币基金数据{len(money_df)}条')
    index_em_df = ak.fund_info_index_em(symbol="全部", indicator="全部")
    fin.info(f'获取到指数基金数据{len(index_em_df)}条')
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
                    result = index_em_df[index_em_df['基金代码'] == code]
                    if not result.empty:
                        fund_name = result.iloc[0]['基金名称']
                    else:
                        result = money_df[money_df['基金代码'] == code]
                        if not result.empty:
                            pass
                        else:
                            fin.error(f"基金：{code} {name}，未找到")
                            continue
                    
            if fund_name:
                if not name.endswith('ETF'):
                    try:
                        fee_df = ak.fund_individual_detail_info_xq(symbol=code)
                        last_sell_rule = fee_df[fee_df['费用类型'] == '卖出规则'].iloc[-1]
                        row[SALE].value = f'{last_sell_rule['条件或名称'].replace("持有期限", "")}({last_sell_rule['费用']})'
                        other_fees_df = fee_df[fee_df['费用类型'] == '其他费用']
                        formula = "=" + "+".join([str(fee) for fee in other_fees_df['费用']])
                        row[FEE].value = formula
                    except Exception:
                        fin.error(f"基金：{code} {name}，获取交易信息失败")
                        continue
                    
                    try:
                        basic_df = ak.fund_individual_basic_info_xq(symbol=code)
                        row[FOUND_DATE].value = basic_df[basic_df['item']=='成立时间'].iloc[0]['value']
                        row[MANAGER].value = basic_df[basic_df['item']=='基金经理'].iloc[0]['value']
                        row[SCALE].value = basic_df[basic_df['item']=='最新规模'].iloc[0]['value']
                    except Exception:
                        fin.error(f"基金：{code} {name}，获取基础信息失败")
                        continue
                
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