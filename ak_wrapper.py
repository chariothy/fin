import akshare as ak
from utils import fu
import pandas as pd
from os import path
from pybeans import utils as ut


class AkShareWrapper:
    def __init__(self, interface: str) -> None:
        self.interface = interface
        self.filename = f'{interface}-{ut.today()}.h5'
        
        
    def __call__(self, *args, **kwargs):
        file_path = fu['tmp_dir'] + '/' + self.filename
            
        if len(list(kwargs.keys())) == 0:
            key = '*'
        else:
            key = kwargs['symbol']
        
        with pd.HDFStore(file_path,'a') as hdf:
            if key in hdf:
                df = hdf.get(key)
            else:
                func = ak.__dict__.get(self.interface)
                print(self.interface, ak.__dict__.keys())
                if callable(func):
                    df = func(**kwargs)
                    hdf.put(key, df, complevel=9)
                else:
                    raise Exception(f'{self.interface} is not callable, func={func}')
            return df