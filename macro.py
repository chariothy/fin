import fire
import akshare as ak
from sqlalchemy import orm, Column, String, DATE, select
from sqlalchemy.dialects.postgresql import insert, JSONB
from utils import fin, today
import numpy as np
import pandas as pd
from notify import ding
from datetime import date
import re
from requests.exceptions import ConnectionError


Base = orm.declarative_base()

class Macro(Base):
    __tablename__ ='macro'
    name = Column(String, primary_key=True, comment='宏观指标')
    data = Column(JSONB, comment='JSON数据')
    updated_at = Column(DATE, comment='更新日期')
            

Base.metadata.create_all(fin.engine) ## Must after orm class


def _save(name, data):
    insert_stmt = insert(Macro).values(name=name, data=data, updated_at=today)
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['name'],
        set_=dict(data=data, updated_at=today)
    )
    with fin.session.begin() as sess:
        sess.execute(update_stmt)


def _updated(name):
    '''
    检查当日是否已经更新
    '''
    # 构建查询语句
    stmt = select(Macro).where(Macro.name == name)
    with fin.session() as sess:
        # 执行查询
        result = sess.scalars(stmt).one_or_none()
        # 检查日期
        if result is not None and result.updated_at == today:
            fin.info(f'"{name}"已是最新')
            return True
    return False


@fin.retry(n=3, error=ConnectionError)
def cpi(slient=False):
    '''消费价格指数
    '''
    name = 'CPI'
    if _updated(name): return
    
    df = ak.macro_china_cpi_yearly()
    fin.debug(df)
    df.loc[:, '日期'] = df['日期'].astype(str)
    json_data = df[['日期', '今值', '预测值', '前值']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[-1]['日期']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
    

@fin.retry(n=3, error=ConnectionError)
def ppi(slient=False):
    '''工业品价格指数
    '''
    name = 'PPI'
    if _updated(name): return
    
    df = ak.macro_china_ppi_yearly()
    fin.debug(df)
    df.loc[:, '日期'] = df['日期'].astype(str)
    json_data = df[['日期', '今值', '预测值', '前值']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[-1]['日期']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
    
    
@fin.retry(n=3, error=ConnectionError)
def pmi(slient=False):
    '''销售经理人指数
    '''
    name = 'PMI'
    if _updated(name): return
    
    df = ak.macro_china_pmi_yearly()
    fin.debug(df)
    df.loc[:, '日期'] = df['日期'].astype(str)
    json_data = df[['日期', '今值', '预测值', '前值']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[-1]['日期']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
    

@fin.retry(n=3, error=ConnectionError)
def money(slient=False):
    '''货币和准货币(M2)
    '''
    name = 'MONEY'
    if _updated(name): return
    
    df = ak.macro_china_money_supply()
    df.loc[:, '月份'] = df['月份'].apply(lambda x: re.sub(r'(\d{4})\D+(\d{2})\D+', r'\1-\2', x))
    df['M1-M2同比增速差'] = (df['货币(M1)-同比增长'] - df['货币和准货币(M2)-同比增长']).round(1)
    fin.debug(df)
    df.info()
    json_data = df[['月份', '货币和准货币(M2)-同比增长', '货币(M1)-同比增长', 'M1-M2同比增速差']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[0]['月份']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
    
    
@fin.retry(n=3, error=ConnectionError)
def retail(slient=False):
    '''社会消费品零售总额
    '''
    name = 'RETAIL'
    if _updated(name): return
    
    df = ak.macro_china_consumer_goods_retail()
    df.loc[:, '月份'] = df['月份'].apply(lambda x: re.sub(r'(\d{4})\D+(\d{2})\D+', r'\1-\2', x))
    fin.debug(df)
    df.info()
    json_data = df[['月份', '同比增长']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[0]['月份']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
   
       
@fin.retry(n=3, error=ConnectionError)
def financing(slient=False):
    '''社会融资规模
    '''
    name = 'FINANCING'
    if _updated(name): return
    
    df = ak.macro_china_shrzgm()
    df.loc[:, '月份'] = df['月份'].apply(lambda x: re.sub(r'(\d{4})(\d{2})', r'\1-\2', x))
    df['日期'] = pd.to_datetime(df['月份'])
    # 设置月份列为索引
    df.set_index('日期', inplace=True)
    df['同比变化'] = df['社会融资规模增量'].pct_change(12) * 100
    
    fin.debug(df)
    df.info()
    json_data = df[['月份', '社会融资规模增量', '同比变化']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[-1]['月份']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
    
    
@fin.retry(n=3, error=ConnectionError)
def leverr(slient=False):
    '''宏观杠杆率
    '''
    name = 'LEVERR'
    if _updated(name): return
    
    df = ak.macro_cnbs()
    fin.debug(df)
    df.info()
    json_data = df[['年份', '居民部门', '非金融企业部门', '政府部门']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[-1]['年份']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
    
    
@fin.retry(n=3, error=ConnectionError)
def bond10(slient=False):
    '''10年国债收益率
    '''    
    name = 'BOND10'
    if _updated(name): return
    
    df = ak.bond_zh_us_rate(start_date="20240918")    # start with A500 - 000510
    df.loc[:, '日期'] = df['日期'].astype(str)
    fin.debug(df)
    df.info()
    json_data = df[['日期', '中国国债收益率10年', '美国国债收益率10年']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[-1]['日期']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
    
        
@fin.retry(n=3, error=ConnectionError)
def shibor(slient=False):
    '''10年国债收益率
    '''    
    name = 'SHIBOR'
    if _updated(name): return
    
    df = ak.rate_interbank(market="上海银行同业拆借市场", symbol="Shibor人民币", indicator="隔夜")
    df.loc[:, '报告日'] = df['报告日'].astype(str)
    fin.debug(df)
    df.info()
    json_data = df[['报告日', '利率', '涨跌']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    _save(name, json_data)

    last = df.iloc[-1]['报告日']
    title = f'{name}更新到{last}'
    if slient:
        return title
    else:
        fin.ding(title,f'共{len(json_data)}行')
        
        
if __name__ == "__main__":
    fire.Fire()