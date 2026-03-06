import polars as pl
import s3fs
import io
import os


def run_pipeline():
    print("🚀 Iniciando Pipeline: S3 Bronze -> S3 Gold")
    minio_host = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    storage_options = {
        "key": "admin",
        "secret": "password123",
        "endpoint_url": f"http://{minio_host}",
        "client_kwargs": {"region_name": "us-east-1"}
    }

    fs = s3fs.S3FileSystem(**storage_options)

    # 1. Search for files (this should not fail)
    print("📥 Cargando datos desde MinIO...")
    files = fs.glob("lakehouse/bronze/*.parquet")

    if not files:
        print("❌ No se encontraron archivos en la capa Bronze.")
        return

    # 2. SAFE READ: Read content in memory before passing it to Polars
    # This prevents Polars from attempting to "normalize" Windows paths
    dfs = []
    for file in files:
        with fs.open(file, mode='rb') as f:
            # Read the entire file into a memory buffer (BytesIO)
            file_content = io.BytesIO(f.read())
            # Polars reads from the buffer, unaware of disk paths
            dfs.append(pl.read_parquet(file_content))

    full_df = pl.concat(dfs)

    # 3. Rest of the pipeline (this no longer touches paths, so it won't fail)
    print("🧹 Procesando datos...")
    silver_df = full_df.filter(
        pl.col("precio_unitario").is_not_null()
    ).with_columns([
        (pl.col("cantidad") * pl.col("precio_unitario")).round(2).alias("venta_total")
    ])

    gold_df = silver_df.group_by("categoria").agg([
        pl.col("venta_total").sum().alias("ingresos_totales"),
        pl.col("transaction_id").count().alias("num_ventas")
    ]).sort("ingresos_totales", descending=True)

    # 4. SAFE WRITE
    print("📊 Guardando resultados en Capa Gold...")
    gold_path = "lakehouse/gold/reporte_categorias.parquet"

    # Write to a buffer first
    buffer = io.BytesIO()
    gold_df.write_parquet(buffer)
    buffer.seek(0)

    # Upload the buffer to MinIO
    with fs.open(gold_path, mode='wb') as f:
        f.write(buffer.read())

    print(f"✅ Pipeline completado con éxito.")
    print(gold_df)


if __name__ == "__main__":
    run_pipeline()
