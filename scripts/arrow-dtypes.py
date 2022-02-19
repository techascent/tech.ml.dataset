import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather

with pa.ipc.open_stream("test/data/alldtypes.arrow-ipc") as reader:
   df = reader.read_pandas()

print(df)

feather.write_feather(df, "test/data/alldtypes.arrow-feather")
feather.write_feather(df, "test/data/alldtypes.arrow-feather-compressed", compression='zstd')

df = df.drop(columns=["local_times"])

feather.write_feather(df, "test/data/alldtypes.arrow-feather-v1", version=1)
