import fire
import akshare as ak
from sqlalchemy import String
from utils import fin, today
from notify import ding
from datetime import date
from requests.exceptions import ConnectionError

from sqlalchemy import orm, Integer, Column, String, DATE, DECIMAL, UniqueConstraint, select, update
from sqlalchemy.dialects.postgresql import insert, JSONB


Base = orm.declarative_base()

ALL_INDEX = None
DB_INDEX = {}

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
    name = Column(String, comment='指数名称')
    data = Column(JSONB, comment='JSON数据')
    updated_at = Column(DATE, comment='更新日期')
       
Base.metadata.create_all(fin.engine) ## Must after orm class
    

def _update_name(code, name):
    '''
    更新指数名称
    '''
    if DB_INDEX[code] is None:
        stmt = (
            update(IndexBase).
            where(IndexBase.code.in_([code])).
            values(name=name)
        )
        with fin.session.begin() as sess:
            sess.execute(stmt)
            fin.info(f'"{code}"名称更新到"{name}"')


def _updated(code):
    '''
    检查当日是否已经更新
    '''
    # 构建查询语句
    stmt = select(IndexBase).where(IndexBase.code == code)
    with fin.session.begin() as sess:
        # 执行查询
        result = sess.scalars(stmt).one_or_none()
        if result is None:
            fin.debug(f'"{code}"未找到，插入index base')
            names = ALL_INDEX[ALL_INDEX['index_code'] == code]['display_name'].values
            if len(names) == 0:
                fin.debug(f'"{code}"未找到')
                name = None
            else:
                name = names[0]
            DB_INDEX[code] = name
            stmt = insert(IndexBase).values(code=code, name=name, data={}, updated_at=today)
            sess.execute(stmt)
        elif result.updated_at == today:
            DB_INDEX[code] = result.name
            fin.info(f'"{code}"已是最新')
            return True
        DB_INDEX[code] = None

    return False


@fin.retry(n=3, error=ConnectionError)
def value():
    '''
    PE - 市盈率
    DY - 股息率
    '''
    global ALL_INDEX
    report = []
    report_date = None
    ALL_INDEX = ak.index_stock_info()

    for index in fin['index']:
        if _updated(index): continue
        
        df = ak.stock_zh_index_value_csindex(symbol=index)
        fin.debug(df)
        
        with fin.session.begin() as sess:
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
            # 更新基金名称
            name_stmt = (
                update(IndexBase).
                where(IndexBase.code.in_([index])).
                values(updated_at=today)
            )
            sess.execute(name_stmt)

        report_date = df.iloc[0]["日期"]
        cur_pe = df.iloc[0]["市盈率2"]
        cur_dy = df.iloc[0]["股息率2"]
        last_pe = df.iloc[1]["市盈率2"]
        last_dy = df.iloc[1]["股息率2"]
        index_name = df.iloc[0]["指数中文简称"]
        delta_pe = cur_pe - last_pe
        delta_dy = cur_dy - last_dy
        _update_name(index, index_name)
        
        fin.info(f'"{index_name}"更新到{report_date}')
        report.append(f'- {index_name} PE:{cur_pe}({delta_pe:+.2f}), 股息率:{cur_dy}({delta_dy:+.2f})')

    fin.debug(report)
    if len(report) > 0:
        fin.ding(f'指数估值{report_date}', '\n'.join(report))


def all(keyword=None, code=None):
    df = ak.index_stock_info()
    print(df)
    if keyword:
        print(df[df['display_name'].str.contains(keyword)])
    if code:
        print(df[df['index_code'] == code])


def history(code):
    df = ak.stock_zh_index_value_csindex(symbol=code)
    print(df)


if __name__ == "__main__":
    fire.Fire()