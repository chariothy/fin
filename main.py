import index
import macro
import index_931995
from utils import fin, today


if __name__ == "__main__":
    try:
        index.value()
        results = [
            index_931995.get_config(slient=True),
            macro.cpi(slient=True),
            macro.ppi(slient=True),
            macro.pmi(slient=True),
            macro.money(slient=True),
            macro.retail(slient=True),
            #macro.financing(slient=True), ## 社融从2025-07-23开始出现SSL错误
            macro.leverr(slient=True),
            macro.bond10(slient=True),
            macro.shibor(slient=True),
            macro.margin(slient=True),
            #macro.sentiment(slient=True), ## 情绪指数从2025-06-08开始出现json解析错误，可能是返回了空字符串
            #macro.index(slient=True), ## 指数估值从1.15.51开始被删除
            #macro.sh300_fear_greed(slient=True), ## 指数估值从1.15.65开始被删除
            macro.sh300_index(slient=True),
        ]
        valid_results = [r for r in results if r is not None]
        fin.debug(valid_results)
        if len(valid_results) > 0:
            fin.ding(f'宏观指标{today}', '\n'.join([f'- {r}' for r in valid_results]))
    except Exception as ex:
        fin.exception(ex)