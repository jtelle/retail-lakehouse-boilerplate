from datetime import datetime
import io
import boto3
import polars as pl
import numpy as np
import random
import time
from datetime import datetime, timedelta

# Dynamic seed
random.seed(int(time.time()))


def generate_massive_supermarket_data(num_tickets=1000):
    # 1. Configuration of 50 stores
    tiendas = [f"TIENDA_{str(i).zfill(2)}" for i in range(1, 51)]

    # 2. Extended inventory
    INVENTARIO = {
        "Alimentación Fresca": ["Leche", "Huevos", "Pollo", "Manzanas", "Pan", "Pescado", "Plátanos"],
        "Despensa": ["Arroz", "Pasta", "Aceite", "Tomate", "Legumbres", "Azúcar", "Sal"],
        "Limpieza": ["Detergente", "Lavavajillas", "Lejía", "Suavizante", "Estropajo"],
        "Cuidado Personal": ["Champú", "Gel", "Dentrífico", "Desodorante", "Jabón"],
        "Bebidas": ["Agua 5L", "Refresco", "Cerveza", "Vino", "Zumo"]
    }

    # 3. Massive generation using vectors (much faster than loops)
    data = []

    # We simulate that each ticket has between 1 and 15 products
    for _ in range(num_tickets):
        tienda_actual = random.choice(tiendas)
        ticket_id = random.randint(10000000, 99999999)
        # Random date in the last 24 hours for realism
        fecha_ticket = datetime.now() - timedelta(minutes=random.randint(0, 1440))

        # Each ticket has several products
        for _ in range(random.randint(1, 15)):
            cat = random.choice(list(INVENTARIO.keys()))
            prod = random.choice(INVENTARIO[cat])

            data.append({
                "ticket_id": ticket_id,
                "timestamp": fecha_ticket.strftime("%Y-%m-%d %H:%M:%S"),
                "tienda_id": tienda_actual,
                "categoria": cat,
                "producto": prod,
                "precio_unitario": round(random.uniform(0.5, 25.0), 2),
                "cantidad": random.randint(1, 6)
            })

    return pl.DataFrame(data)


def save_to_minio(df):
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"ventas_super_{timestamp_str}.parquet"

    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )

    # --- NUEVO: Verificar y crear el bucket si no existe ---
    bucket_name = 'lakehouse'
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        print(f"🪣 El bucket '{bucket_name}' no existe. Creándolo...")
        s3.create_bucket(Bucket=bucket_name)
    # -------------------------------------------------------

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    try:
        s3.put_object(Bucket=bucket_name,
                      Key=f'bronze/{filename}', Body=buffer.getvalue())
        print(f"✅ ÉXITO: {filename} subido a MinIO")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    # We generate 2000 tickets per execution (approx. 15,000 - 20,000 rows)
    batch_data = generate_massive_supermarket_data(num_tickets=2000)
    save_to_minio(batch_data)
