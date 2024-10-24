import index
import macro
from utils import fin


if __name__ == "__main__":
    try:
        index.value()
        macro.cpi()
        macro.ppi()
        macro.pmi()
        macro.money()
        macro.retail()
    except Exception as ex:
        fin.exception(ex)