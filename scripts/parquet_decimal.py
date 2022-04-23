import pyarrow.parquet as pq
import pyarrow as pa
d = {'name': ['sample1', 'sample2'], 'decimals': [ 3.4199, 1.2455] }
table = pa.Table.from_pydict(d)
table = table.set_column(1, 'decimals', table.column('decimals').cast(pa.decimal128(12,9)))
pq.write_table(table, 'test/data/decimaltable.parquet')
