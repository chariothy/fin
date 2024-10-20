import fire
import akshare as ak
from sqlalchemy import String
from utils import fin
from notify import ding

from sqlalchemy import Integer, Column, String, DECIMAL, orm, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert

Base = orm.declarative_base()

class IndexValue(Base):
    __tablename__ ='index_value'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='自增id')
    code = Column(String, comment='指数代码')
    date = Column(String, comment='估值日期')
    pe1 = Column(DECIMAL, comment='市盈率1')
    pe2 = Column(DECIMAL, comment='市盈率2')
    dy1 = Column(DECIMAL, comment='股息率1')
    dy2 = Column(DECIMAL, comment='股息率2')
    
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
    )
        
Base.metadata.create_all(fin.engine) 
    
    
def value():
    '''
    PE - 市盈率
    DY - 股息率
    '''
    report = []
    report_date = None
    for index in fin['index']:
        df = ak.stock_zh_index_value_csindex(symbol=index)
        fin.debug(df)
        
        with fin.session() as sess:
            for _, row in df.iterrows():
                insert_stmt = insert(IndexValue).values( \
                        code=index, \
                        date=str(row['日期']), \
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
        report.append(f'- {index_name} PE:{cur_pe}({delta_pe:+.2f}), 股息率:{cur_dy}({delta_dy:+.2f})')

    fin.debug(report)
    if len(report) > 0:
        ding(f'指数估值{report_date}', '\n'.join(report))

if __name__ == "__main__":
    fire.Fire()