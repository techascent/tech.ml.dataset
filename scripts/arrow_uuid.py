import pyarrow as pa
import uuid as uuid

schema = pa.schema([pa.field('id', pa.uuid())])
data = [uuid.UUID("8be643d6-0df7-4e5e-837c-f94170c87914").bytes,
        uuid.UUID("24bc9cf4-e2e8-444f-bb2d-82394f33ff76").bytes,
        uuid.UUID("e8149e1b-aef6-4671-b1b4-3b7a21eed92a").bytes]

with pa.OSFile('test/data/uuid_ext.arrow', 'wb') as sink:
    with pa.ipc.new_file(sink, schema=schema) as writer:
        batch = pa.record_batch([data], schema=schema)
        writer.write(batch)
