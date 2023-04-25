import pandas as pd
out_df = pd.DataFrame().reset_index()
out_df.to_feather("test/data/empty.arrow")
