#!/usr/bin/python
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather


data = pd.read_csv("stocks.csv", index_col='date', parse_dates=True)
arrow_data = pa.Table.from_pandas(data)
feather.write_feather(data,"stocks.pyarrow.feather")

with pa.output_stream("stocks.pyarrow.stream") as f:
    batch=pa.record_batch(data)
    writer = pa.ipc.new_stream(f, batch.schema)
    writer.write_batch(batch)


data = pd.read_csv("../../data/ames-house-prices/train.csv")
arrow_data = pa.Table.from_pandas(data)
with pa.output_stream("ames.pyarrow.stream") as f:
    batch=pa.record_batch(data)
    writer = pa.ipc.new_stream(f, batch.schema)
    writer.write_batch(batch)

