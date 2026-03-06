'''import polars as pl


def run_pipeline():
    options = {
        "key": "admin",
        "secret": "password123",
        "endpoint_url": "http://minio:9000",
        "use_ssl": "false",
        "region": "us-east-1",  # Aunque sea local, a veces lo pide para no buscar en AWS
        "s3_addressing_style": "path",
        # Esto bloquea el intento de conexión externa
        "token_metadata_service_url": "http://127.0.0.1"
    }

    print("Reading all bronze data...")
    # We read EVERYTHING using the wildcard *
    df = pl.read_parquet("s3://lakehouse/bronze/*.parquet",
                         storage_options=options)

    # We create the Total Sale column (Price * Quantity)
    df = df.with_columns((pl.col("precio_unitario") *
                         pl.col("cantidad")).alias("venta_total"))

    print("Creating Gold Layer: Store Performance...")
    # We aggregate by Store and Category for the Dashboard
    df_gold = df.group_by(["tienda_id", "categoria"]).agg([
        pl.sum("venta_total").alias("ingresos_totales"),
        pl.n_unique("ticket_id").alias("total_tickets"),
        pl.sum("cantidad").alias("unidades_vendidas"),
        pl.mean("precio_unitario").alias("ticket_medio_unitario")
    ])

    # We save the final result (it will weigh much less than the original)
    path_gold = "s3://lakehouse/gold/store_report.parquet"
    df_gold.write_parquet(path_gold, storage_options=options)
    print(f"✅ Gold Layer updated: {path_gold}")


if __name__ == "__main__":
    run_pipeline()
'''

import polars as pl
import boto3
import io


def run_pipeline():
    # 1. Configuración de conexión (Igual que el generador)
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )

    bucket_name = 'lakehouse'

    print("🔍 Buscando datos en la capa Bronze...")

    try:
        # Listamos los archivos dentro de la carpeta bronze
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix='bronze/')

        if 'Contents' not in response:
            print("⚠️ No se encontraron archivos en Bronze. Abortando pipeline.")
            return

        all_dfs = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                # Descargamos el archivo a memoria
                file_obj = s3_client.get_object(
                    Bucket=bucket_name, Key=obj['Key'])
                df_temp = pl.read_parquet(io.BytesIO(file_obj['Body'].read()))
                all_dfs.append(df_temp)

        # Unimos todos los archivos acumulados
        df = pl.concat(all_dfs)

        # 2. Transformación para el Dashboard (Capa Gold)
        print(f"📊 Procesando {len(df)} registros para la Capa Gold...")

        # Calculamos ingresos por tienda y categoría
        df_gold = df.with_columns(
            (pl.col("precio_unitario") * pl.col("cantidad")).alias("venta_total")
        ).group_by(["tienda_id", "categoria"]).agg([
            pl.sum("venta_total").alias("ingresos_totales"),
            pl.n_unique("ticket_id").alias("total_tickets")
        ])

        # 3. Guardar el reporte final en Gold
        buffer = io.BytesIO()
        df_gold.write_parquet(buffer)
        buffer.seek(0)

        s3_client.put_object(
            Bucket=bucket_name,
            Key='gold/store_report.parquet',
            Body=buffer.getvalue()
        )
        print("✅ Pipeline finalizado: Capa Gold actualizada.")

    except Exception as e:
        print(f"❌ Error crítico en el Pipeline: {e}")


if __name__ == "__main__":
    run_pipeline()
