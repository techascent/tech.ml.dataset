import pandas as pd
import pyarrow.feather as ft

df = pd.DataFrame({'idx': [0, 1, 2],
                   'bytedata': [b'\x7f\x45\x4c\x46\x01\x01\x01\x00', b'\x7f\x45\x4c\x46\x01\x01\x01\x00', b'\x7f\x45\x4c\x46\x01\x01\x01\x00']
                   })

df.to_feather("test/data/arrow_bytes.arrow" )
