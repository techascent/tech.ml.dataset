#!/usr/bin/python3

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
rows = [{"id": 1,
         "val":[("a",{"weight":41.5, "temp":36.1}),
                ("b",{"weight":31.6,"temp":34.5})],
         "val2":[("va", {"weight":2, "temp":3}),
                 ("vb", {"weight":3, "temp":4})]},
        {"id": 2,
         "val":[("a",{"weight":11.5, "temp":22.1}),
                ("b",{"weight":31.6,"temp":34.5})]},
        {"id": 3,
         "val":[("a",{"weight":22.5,"temp":33.1}),
                ("b",{"weight":33.6, "temp":44.5}),
                ("c",{"weight":44.6, "temp":55.5})],
         "val2":[("vb", {"weight":5, "temp":10})]
        }]
df2 = pd.DataFrame(rows)
mystruct = pa.struct([pa.field("weight", pa.float32()),
                      pa.field("temp", pa.float32())])
mymap = pa.map_(pa.string(), mystruct)
schema = pa.schema([pa.field('id', pa.int32()), pa.field('val', mymap), pa.field("val2", mymap)])
print(schema)
table = pa.Table.from_pandas(df2, schema)
pq.write_table(table, 'test/data/nested.parquet')
