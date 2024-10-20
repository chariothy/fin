import fire
import akshare as ak
from sqlalchemy import Column, String, orm
from sqlalchemy.dialects.postgresql import insert, JSONB
from utils import fin
import numpy as np
from notify import ding

Base = orm.declarative_base()

class Macro(Base):
    __tablename__ ='macro'
    name = Column(String, primary_key=True, comment='宏观指标')
    data = Column(JSONB, comment='JSON数据')
            

Base.metadata.create_all(fin.engine) ## Must after Macro orm class


def save(name, data):
    insert_stmt = insert(Macro).values(name=name, data=data)
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['name'],
        set_=dict(data=data)
    )
    with fin.session() as sess:
        sess.execute(update_stmt)
        sess.commit()


def cpi():
    '''
    '''
    macro_china_cpi_yearly_df = ak.macro_china_cpi_yearly()
    fin.debug(macro_china_cpi_yearly_df)
    macro_china_cpi_yearly_df.loc[:, '日期'] = macro_china_cpi_yearly_df['日期'].astype(str)
    json_data = macro_china_cpi_yearly_df[['日期', '今值', '预测值', '前值']] \
        .replace(np.nan, None) \
        .values \
        .tolist()
    
    save('cpi', json_data)

    last = macro_china_cpi_yearly_df.iloc[-1]['日期']
    fin.ding(f'CPI更新{last}',f'共{len(json_data)}行')

if __name__ == "__main__":
    fire.Fire()