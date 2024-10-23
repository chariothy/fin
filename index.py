import fire
import akshare as ak
from sqlalchemy import String
from utils import fin
from notify import ding
from datetime import date
from requests.exceptions import ConnectionError

from sqlalchemy import orm, Integer, Column, String, DATE, DECIMAL, UniqueConstraint, select
from sqlalchemy.dialects.postgresql import insert, JSONB

# 获取当前日期
today = date.today()
Base = orm.declarative_base()

class IndexValue(Base):
    __tablename__ ='index_value'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='自增id')
    code = Column(String, comment='指数代码')
    date = Column(DATE, comment='估值日期')
    pe1 = Column(DECIMAL, comment='市盈率1')
    pe2 = Column(DECIMAL, comment='市盈率2')
    dy1 = Column(DECIMAL, comment='股息率1')
    dy2 = Column(DECIMAL, comment='股息率2')
    
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
    )

class IndexBase(Base):
    __tablename__ ='index_base'
    code = Column(String, primary_key=True, comment='指数代码')
    data = Column(JSONB, comment='JSON数据')
    updated_at = Column(DATE, comment='更新日期')
       
Base.metadata.create_all(fin.engine) ## Must after orm class
    

def updated(code):
    '''
    检查当日是否已经更新
    '''
    # 构建查询语句
    stmt = select(IndexBase).where(IndexBase.code == code)
    with fin.session() as sess:
        # 执行查询
        result = sess.scalars(stmt).one_or_none()
        if result is None:
            stmt = insert(IndexBase).values(code=code, data={}, updated_at=today)
            with fin.session() as sess:
                sess.execute(stmt)                            
                sess.commit()
        elif result.updated_at == today:
            fin.info(f'"{code}"已是最新')
            return True
    return False


@fin.retry(n=3, error=ConnectionError)
def value():
    '''
    PE - 市盈率
    DY - 股息率
    '''
    report = []
    report_date = None
    for index in fin['index']:
        if updated(index): continue
        
        df = ak.stock_zh_index_value_csindex(symbol=index)
        fin.debug(df)
        
        with fin.session() as sess:
            for _, row in df.iterrows():
                insert_stmt = insert(IndexValue).values( \
                        code=index, \
                        date=row['日期'], \
                        pe1=row['市盈率1'], \
                        pe2=row['市盈率2'], \
                        dy1=row['股息率1'], \
                        dy2=row['股息率2']
                )
                update_stmt = insert_stmt.on_conflict_do_nothing(
                    index_elements=['code', 'date']
                )
                sess.execute(update_stmt)                            
            sess.commit()
        report_date = df.iloc[0]["日期"]
        cur_pe = df.iloc[0]["市盈率2"]
        cur_dy = df.iloc[0]["股息率2"]
        last_pe = df.iloc[1]["市盈率2"]
        last_dy = df.iloc[1]["股息率2"]
        index_name = df.iloc[0]["指数中文简称"]
        delta_pe = cur_pe - last_pe
        delta_dy = cur_dy - last_dy
        
        fin.info(f'"{index_name}"更新到{report_date}')
        report.append(f'- {index_name} PE:{cur_pe}({delta_pe:+.2f}), 股息率:{cur_dy}({delta_dy:+.2f})')

    fin.debug(report)
    if len(report) > 0:
        fin.ding(f'指数估值{report_date}', '\n'.join(report))

if __name__ == "__main__":
    fire.Fire()