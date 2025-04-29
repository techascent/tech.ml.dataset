import pyarrow as pa
import uuid as uuid

schema = pa.schema([pa.field('id', pa.decimal128(5, 2))])
data = [1, 0, 2]
table = pa.Table.from_arrays([data], schema=schema)


with pa.OSFile('test/data/bigdec.arrow', 'wb') as sink:
    with pa.ipc.new_file(sink, schema=schema) as writer:
        batch = pa.record_batch([data], schema=schema)
        writer.write(batch)

with pa.memory_map('test/data/bigdec.arrow', 'r') as source:
    loaded_arrays = pa.ipc.open_file(source).read_all()

print(loaded_arrays[0])
