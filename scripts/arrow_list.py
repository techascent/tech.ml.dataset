import pandas as pd
import pyarrow.feather as ft

df_list = pd.DataFrame({'idx': [0, 1, 2],
                        'class-name': [['dog', 'car'], ['dog', 'flower'], ['car', 'flower']],
                        'confidence': [[0.8, 0.3], [0.75, 0.85], [0.46, 0.84]],
                        })

df_list.to_feather("test/data/arrow_list.arrow")
