#!/usr/bin/python

import pyarrow as pa
import pyarrow.feather as feather

my_schema = pa.schema([
    pa.field('year', pa.int64()),
    pa.field('nullcol', pa.null()),
    pa.field('day', pa.int64()),])
pylist = [{'year': 2020, 'nullcol': None, 'day':24}]
table = pa.Table.from_pylist(pylist, schema=my_schema)
feather.write_feather(table, "test/data/withnullcol.arrow", compression="zstd", version=2)
