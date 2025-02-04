import akshare as ak
import openpyxl
import datetime
import shutil
import pandas as pd
from utils import fin

CH_INT = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5}
CODE = 0
NAME = 1
FOUND_DATE = 3
VALUE = 6
SCALE = 7
SALE = 8
FEE = 9
MANAGER = 10
MS_RANK = 12
ALL_RETURN = 13


def set_value(ws_cell, df_cell, divider=100):
    if not pd.isna(df_cell):
        ws_cell.value = df_cell / divider


def get_fund_info():
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    file_path = fin['asset_config_path']
    file_path = file_path.replace('.xlsx', f'-{current_date}.xlsx')
    try:
        shutil.copyfile(fin['asset_config_path'], file_path)
    except PermissionError:
        print("文件被占用，请关闭后重试...")
        return
    
    off_exchg_df = ak.fund_open_fund_rank_em(symbol="全部")
    fin.info(f'获取到场外基金数据{len(off_exchg_df)}条')
    on_exchg_df = ak.fund_exchange_rank_em()
    fin.info(f'获取到场内基金数据{len(on_exchg_df)}条')
    money_df = ak.fund_money_rank_em()
    fin.info(f'获取到货币基金数据{len(money_df)}条')
    index_em_df = ak.fund_info_index_em(symbol="全部", indicator="全部")
    fin.info(f'获取到指数基金数据{len(index_em_df)}条')
    #print(df)
    
    wb = openpyxl.load_workbook(file_path)
    try:
        sheet = wb['基金']
        data = []
        for row in sheet.iter_rows(min_row=2):
            #print(row)
            code = row[CODE].value
            name = row[NAME].value
            manager = row[MANAGER].value
            all_return_cell = row[ALL_RETURN]
            
            if not code:
                continue
            
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
                            fin.info(f"未找到基金：{code} {name}")
                            continue
                    
            if fund_name:
                if fund_name != name:
                    fin.error(f"基金名称发生变化 - 基金：{code}：{name} -> {fund_name}")
                #fin.debug(f"找到基金：{code} {name}")
                
                set_value(row[VALUE], result.iloc[0]['单位净值'], 1)
                set_value(all_return_cell, result.iloc[0]['成立来'])
                set_value(all_return_cell.offset(column=1), result.iloc[0]['近1周'])
                set_value(all_return_cell.offset(column=2), result.iloc[0]['近1月'])
                set_value(all_return_cell.offset(column=3), result.iloc[0]['近3月'])
                set_value(all_return_cell.offset(column=4), result.iloc[0]['近6月'])
                set_value(all_return_cell.offset(column=5), result.iloc[0]['近1年'])
                set_value(all_return_cell.offset(column=6), result.iloc[0]['近2年'])
                set_value(all_return_cell.offset(column=7), result.iloc[0]['近3年'])
                set_value(all_return_cell.offset(column=8), result.iloc[0]['今年来'])
                
                try:
                    fee_df = ak.fund_individual_detail_info_xq(symbol=code)
                    last_sell_rule = fee_df[fee_df['费用类型'] == '卖出规则'].iloc[-1]
                    sale_fee = ''
                    if last_sell_rule['费用'] > 0:
                        sale_fee = f'({last_sell_rule['费用']})'
                    row[SALE].value = f'{last_sell_rule['条件或名称'].replace("持有期限", "").replace("<=", "")}{sale_fee}'
                    other_fees_df = fee_df[fee_df['费用类型'] == '其他费用']
                    formula = "=(" + "+".join([str(fee) for fee in other_fees_df['费用']]) + ")/100"
                    row[FEE].value = formula
                except Exception as ex:
                    #raise ex
                    fin.info(f"交易信息获取失败 - 基金：{code} {name}, {str(ex)}")
                    continue
                
                try:
                    basic_df = ak.fund_individual_basic_info_xq(symbol=code)
                    row[FOUND_DATE].value = basic_df[basic_df['item']=='成立时间'].iloc[0]['value']
                    new_manager = basic_df[basic_df['item']=='基金经理'].iloc[0]['value']
                    if new_manager != manager:
                        row[MANAGER].value = new_manager
                        if manager:
                            fin.error(f"基金经理发生变化 - 基金：{code} {name}：{manager} -> {new_manager}")
                        
                    scale = basic_df[basic_df['item']=='最新规模'].iloc[0]['value']
                    scale = scale.replace("亿", "")
                    if scale.endswith("万"):
                        scale = float(scale.replace("万", "")) / 10000
                    row[SCALE].value = float(scale)

                    rating_by = basic_df[basic_df['item'] == '评级机构'].iloc[0]['value']                        
                    if pd.notna(rating_by): # and rating_by == '晨星评级':
                        rating = basic_df[basic_df['item'] == '基金评级'].iloc[0]['value']    
                        num = CH_INT[rating.split('星')[0]]
                        row[MS_RANK].value = '★' * num + rating_by
                except Exception as ex:
                    #raise ex
                    fin.info(f"基础信息获取失败 - 基金：{code} {name}， {str(ex)}")
                    continue                
            else:
                try:
                    hist_df = ak.fund_open_fund_info_em(symbol=code, indicator="累计净值走势")
                    set_value(row[VALUE], hist_df.iloc[-1]['累计净值'], 1)
                    hist_df = ak.fund_open_fund_info_em(symbol=code, indicator="累计收益率走势", period="成立来")
                    set_value(all_return_cell, hist_df.iloc[-1]['累计收益率'])
                except Exception as ex:
                    #raise ex
                    fin.info(f"收益信息获取失败 - 基金：{code} {name}， {str(ex)}")
                    continue
        wb.save(file_path)
    finally:
        wb.close()

if __name__ == "__main__":
    get_fund_info()
    input('Press any key to quit')