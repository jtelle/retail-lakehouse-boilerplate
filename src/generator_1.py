import polars as pl
import numpy as np
import pathlib
import s3fs
import os


def generate_retail_data(n_records=1_000_000, n_files=5, use_cloud=True):
    print(f"🚀 Iniciando generación de {n_records} registros...")

    # 1. MinIO (Local S3) connection configuration
    # Parameter names expected by s3fs directly, not boto3
    # Read the endpoint from the environment variable; if not exists, use localhost
    minio_host = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    storage_options = {
        "key": "admin",
        "secret": "password123",
        "endpoint_url": f"http://{minio_host}",
        "client_kwargs": {"region_name": "us-east-1"}
    }

    # 2. Synthetic dataset params
    tiendas = [f"TIENDA_{i:03d}" for i in range(1, 51)]
    categorias = ["Alimentación", "Hogar", "Electrónica", "Textil", "Mascotas"]
    records_per_file = n_records // n_files

    # Range of dates 2024 in microseconds (Avoids the MemoryError of 229 TiB)
    start_ts = np.datetime64(
        '2024-01-01').astype('datetime64[us]').astype(np.int64)
    end_ts = np.datetime64(
        '2025-01-01').astype('datetime64[us]').astype(np.int64)
    rng = np.random.default_rng()

    # 3. Initialize S3 File System
    fs = None
    if use_cloud:
        # Create the connection with s3fs
        fs = s3fs.S3FileSystem(**storage_options)

        # Verify/Create the bucket to avoid the "Location is empty" message
        if not fs.exists("lakehouse"):
            fs.mkdir("lakehouse")
            print("📁 Bucket 'lakehouse' creado.")
        print("🔗 Conectado exitosamente a MinIO")

    # 4. File generation loop
    for i in range(n_files):
        # Random timestamps in 2024 (in microseconds to avoid overflow)
        random_ints = rng.integers(
            start_ts, end_ts, size=records_per_file, dtype=np.int64)

        data = pl.DataFrame({
            "transaction_id": rng.integers(100000, 999999, size=records_per_file),
            "timestamp": random_ints.astype('datetime64[us]'),
            "tienda_id": rng.choice(tiendas, records_per_file),
            "producto_id": rng.integers(1000, 5000, size=records_per_file),
            "categoria": rng.choice(categorias, records_per_file),
            "cantidad": rng.integers(1, 10, size=records_per_file),
            "precio_unitario": rng.uniform(0.5, 100.0, size=records_per_file).round(2),
        })

        # Inject 2% of null values into the price to simulate “dirty” data.
        data = data.with_columns([
            pl.when(pl.Series(rng.random(records_per_file)) > 0.98)
              .then(None)
              .otherwise(pl.col("precio_unitario"))
              .alias("precio_unitario")
        ])

        # 5. Data writing (Cloud or Local)
        if use_cloud:
            # Force the path to be compatible with S3 (avoid Windows path issues)
            cloud_path = f"s3://lakehouse/bronze/sales_part_{i}.parquet"

            # Open a file in write-binary mode and write the parquet data directly to it
            with fs.open(cloud_path, mode='wb') as f:
                data.write_parquet(f)
            print(f"✅ Archivo {i+1}/{n_files} subido a: {cloud_path}")
        else:
            local_path = pathlib.Path(f"data/bronze/sales_part_{i}.parquet")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            data.write_parquet(local_path)
            print(
                f"✅ Archivo {i+1}/{n_files} guardado localmente: {local_path}")

    print("\n✨ Proceso de generación completado.")


if __name__ == "__main__":
    # If you want to generate the data locally, set use_cloud=False and ensure the 'data/bronze' directory exists.
    generate_retail_data(n_records=1_000_000, n_files=5, use_cloud=True)
