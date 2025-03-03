import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


def ingest_zone(url, table_name, output_path):
    df = pd.read_csv(url)

    schema = pa.schema([
        pa.field('LocationID', pa.int32()),
        pa.field('Borough', pa.string()),
        pa.field('Zone', pa.string()),
        pa.field('service_zone', pa.string())
        ])

    table = pa.Table.from_pandas(df, schema=schema)

    parquet_file = os.path.join(output_path, "taxi_zone.parquet")

    # Ensure the directory exists
    os.makedirs(output_path, exist_ok=True)

    # Save as a single Parquet file
    pq.write_table(table, parquet_file)