import index
import macro
from utils import fin, today


if __name__ == "__main__":
    try:
        index.value()
        results = [
            macro.cpi(slient=True),
            macro.ppi(slient=True),
            macro.pmi(slient=True),
            macro.money(slient=True),
            macro.retail(slient=True),
            macro.financing(slient=True),
            macro.leverr(slient=True),
            macro.bond10(slient=True),
            macro.shibor(slient=True),
            macro.margin(slient=True),
            macro.sentiment(slient=True),
            #macro.index(slient=True),
            macro.sh300_fear_greed(slient=True)
        ]
        valid_results = [r for r in results if r is not None]
        fin.debug(valid_results)
        if len(valid_results) > 0:
            fin.ding(f'宏观指标{today}', '\n'.join([f'- {r}' for r in valid_results]))
    except Exception as ex:
        fin.exception(ex)