#!/usr/bin/python

import pyarrow as pa
import pyarrow.feather as feather

my_schema = pa.schema([
    pa.field('year', pa.int64()),
    pa.field('nullcol', pa.null())])
pylist = [{'year': 2020, 'nullcol': None}]
table = pa.Table.from_pylist(pylist, schema=my_schema)
feather.write_feather(table, "test/data/withnullcol.arrow", compression="zstd", version=2)
