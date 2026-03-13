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
