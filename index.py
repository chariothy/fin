import fire
import akshare as ak
from sqlalchemy import String
from utils import fin


def value():
    '''
    PE - 市盈率
    DY - 股息率
    '''
    report = []
    for index in fin['index']:
        stock_zh_index_value_csindex_df = ak.stock_zh_index_value_csindex(symbol=index)
        fin.debug(stock_zh_index_value_csindex_df)
        
        # 选择和重命名列
        stock_zh_index_value_csindex_df = stock_zh_index_value_csindex_df[['日期', '指数代码', '市盈率1', '市盈率2', '股息率1', '股息率2']]
        stock_zh_index_value_csindex_df.columns = ['date', 'code', 'pe1', 'pe2', 'dy1', 'dy2']
        stock_zh_index_value_csindex_df['code'] = stock_zh_index_value_csindex_df['code'].astype(str)
        stock_zh_index_value_csindex_df['code'] = stock_zh_index_value_csindex_df['code'].apply(lambda x: x.zfill(6))
        
        inserted = fin.save(stock_zh_index_value_csindex_df, 'index_value', ('date', 'code'), {'code': String})
        if inserted > 0:
            report.append(f'- {index} inserted {inserted}')

    fin.debug(report)
    if len(report) > 0:
        fin.ding('指数', '\n'.join(report))

if __name__ == "__main__":
    fire.Fire()