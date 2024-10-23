import fire
import akshare as ak
from sqlalchemy import orm, Column, String, DATE, select
from sqlalchemy.dialects.postgresql import insert, JSONB
from utils import fin
import numpy as np
from notify import ding
from datetime import date
import re
from requests.exceptions import ConnectionError



# 获取当前日期
today = date.today()
Base = orm.declarative_base()

class Macro(Base):
    __tablename__ ='macro'
    name = Column(String, primary_key=True, comment='宏观指标')
    data = Column(JSONB, comment='JSON数据')
    updated_at = Column(DATE, comment='更新日期')
            

Base.metadata.create_all(fin.engine) ## Must after orm class


def save(name, data):
    insert_stmt = insert(Macro).values(name=name, data=data, updated_at=today)
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['name'],
        set_=dict(data=data, updated_at=today)
    )
    with fin.session() as sess:
        sess.execute(update_stmt)
        sess.commit()


def updated(name):
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
def cpi():
    '''
    '''
    name = 'CPI'
    if updated(name): return
    
    df = ak.macro_china_cpi_yearly()
    fin.debug(df)
    df.loc[:, '日期'] = df['日期'].astype(str)
    json_data = df[['日期', '今值', '预测值', '前值']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    save(name, json_data)

    last = df.iloc[-1]['日期']
    fin.info(f'"{name}"更新到{last}')
    fin.ding(f'{name}更新{last}',f'共{len(json_data)}行')
    

@fin.retry(n=3, error=ConnectionError)
def ppi():
    '''
    '''
    name = 'PPI'
    if updated(name): return
    
    df = ak.macro_china_ppi_yearly()
    fin.debug(df)
    df.loc[:, '日期'] = df['日期'].astype(str)
    json_data = df[['日期', '今值', '预测值', '前值']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    save(name, json_data)

    last = df.iloc[-1]['日期']
    fin.info(f'"{name}"更新到{last}')
    fin.ding(f'{name}更新{last}',f'共{len(json_data)}行')
    
    
@fin.retry(n=3, error=ConnectionError)
def pmi():
    '''
    '''
    name = 'PMI'
    if updated(name): return
    
    df = ak.macro_china_pmi_yearly()
    fin.debug(df)
    df.loc[:, '日期'] = df['日期'].astype(str)
    json_data = df[['日期', '今值', '预测值', '前值']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    save(name, json_data)

    last = df.iloc[-1]['日期']
    fin.info(f'"{name}"更新到{last}')
    fin.ding(f'{name}更新{last}',f'共{len(json_data)}行')
    

@fin.retry(n=3, error=ConnectionError)
def money():
    '''
    '''
    name = 'MONEY'
    if updated(name): return
    
    df = ak.macro_china_money_supply()
    df.loc[:, '月份'] = df['月份'].apply(lambda x: re.sub(r'(\d{4})\D+(\d{2})\D+', r'\1-\2', x))
    fin.debug(df)
    df.info()
    json_data = df[['月份', '货币和准货币(M2)-同比增长', '货币(M1)-同比增长']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    save(name, json_data)

    last = df.iloc[-1]['月份']
    fin.info(f'"{name}"更新到{last}')
    fin.ding(f'{name}更新{last}',f'共{len(json_data)}行')
    
    
@fin.retry(n=3, error=ConnectionError)
def retail():
    '''
    '''
    name = 'RETAIL'
    if updated(name): return
    
    df = ak.macro_china_consumer_goods_retail()
    df.loc[:, '月份'] = df['月份'].apply(lambda x: re.sub(r'(\d{4})\D+(\d{2})\D+', r'\1-\2', x))
    fin.debug(df)
    df.info()
    json_data = df[['月份', '同比增长']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    save(name, json_data)

    last = df.iloc[-1]['月份']
    fin.info(f'"{name}"更新到{last}')
    fin.ding(f'{name}更新{last}',f'共{len(json_data)}行')
    
    
if __name__ == "__main__":
    fire.Fire()