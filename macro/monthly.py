import akshare as ak
import numpy as np
from pandas import DataFrame
from utils import fu, parse_ym
from schema.economy import EconomyMonthly
from pybeans import utils as pu


def monthly_all():
    cpi()
    ppi()
    account()
    cgr()
    money()
    

@pu.benchmark
def save(df: DataFrame, key: str, map: dict):
    """通用保存函数

    Args:
        df (DateFrame): 原始数据
        key (str): 关键列
        map (dict): DataFrame
    """
    df = df.replace({np.nan: None})
    latest_db = fu.session.query(EconomyMonthly) \
        .filter(getattr(EconomyMonthly, key) != None) \
        .order_by(EconomyMonthly.date.desc()) \
        .first()
    
    if latest_db:
        fu.D('Found latest date for ', key, latest_db.date)
        new_df = df[df.date > latest_db.date]
        fu.D(new_df)
    else:
        fu.D('No ', key, 'yet')
        new_df = df

    for index, row in new_df.iterrows():
        economy = fu.session.query(EconomyMonthly).filter_by(date=row.date).one_or_none()
        if economy is None:
            economy = EconomyMonthly()
        economy.date = row.date
        for k in map:
            v = map[k]
            setattr(economy, k, row[v])
        fu.session.add(economy)
        
    try:
        fu.session.commit()
    except Exception as ex:
        fu.session.rollback()
        fu.F('数据库错误', ex)
        
        
def cpi():
    """cpi
    """
    df = ak.macro_china_cpi()
    #print(df)
    
    df['date'] = df.月份.apply(parse_ym)
    save(df, 'cpi', {
        'cpi': '全国-当月', 
        'cpi_yy': '全国-同比增长', 
        'cpi_mm': '全国-环比增长',
        'cpi_acc': '全国-累计'
        }
    )
    

def ppi():
    """ppi
    """
    df = ak.macro_china_ppi()
    #print(df)
    
    df['date'] = df.月份.apply(parse_ym)
    save(df, 'ppi', {
        'ppi': '当月', 
        'ppi_yy': '当月同比增长', 
        'ppi_acc': '累计'
        }
    )
    

def account():
    """股票账户
    """
    df = ak.stock_account_statistics_em()
    #print(df)
    
    df['date'] = df.数据日期
    save(df, 'new_investor', {
        'new_investor': '新增投资者-数量', 
        'new_investor_yy': '新增投资者-同比', 
        'new_investor_mm': '新增投资者-环比', 
        'hs_total': '沪深总市值', 
        'sh_point': '上证指数-收盘',
        'sh_point_delta': '上证指数-涨跌幅'
        }
    )
    
    
def cgr():
    """社会消费品零售总额
    """
    df = ak.macro_china_consumer_goods_retail()
    #print(df)
    
    df['date'] = df.月份.apply(parse_ym)
    save(df, 'cgr', {
        'cgr': '当月', 
        'cgr_yy': '同比增长', 
        'cgr_mm': '环比增长', 
        'cgr_acc': '累计',
        'cgr_acc_yy': '累计-同比增长'
        }
    )

def money():
    """货币供应量
    """
    df = ak.macro_china_supply_of_money()
    #print(df)
    
    df['date'] = df.统计时间.apply(parse_ym)
    save(df, 'm1', {
        'm2': '货币和准货币（广义货币M2）', 
        'm2_yy': '货币和准货币（广义货币M2）同比增长',
        'm1': '货币(狭义货币M1)', 
        'm1_yy': '货币(狭义货币M1)同比增长',
        'm0': '流通中现金(M0)', 
        'm0_yy': '流通中现金(M0)同比增长'
        }
    )
    