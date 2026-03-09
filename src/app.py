
import random
import streamlit as st
import polars as pl
import boto3
import io


st.set_page_config(page_title="Supermarket Analytics", layout="wide")

st.title("🛒 Dashboard de Ventas - 50 Tiendas")

# 1. Configuración de conexión idéntica a los otros scripts


@st.cache_data
def load_gold_data():
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )

    bucket_name = 'lakehouse'
    key = 'gold/store_report.parquet'

    try:
        # Descargamos el reporte de la capa Gold
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pl.read_parquet(io.BytesIO(response['Body'].read()))
        return df
    except Exception as e:
        st.error(f"Ocurrió un error al leer los datos: {e}")
        return None


df = load_gold_data()

if df is not None:
    # --- MÉTRICAS PRINCIPALES ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Ingresos Totales", f"{df['ingresos_totales'].sum():,.2f} €")
    col2.metric("Tickets Procesados", f"{df['total_tickets'].sum():,}")
    col3.metric("Tiendas Activas", df["tienda_id"].n_unique())

    # --- GRÁFICO DE VENTAS POR TIENDA ---
    st.subheader("Ranking de Ventas por Tienda")
    # Agrupamos por tienda (por si hay varias categorías por tienda)
    ventas_tienda = df.group_by("tienda_id").agg(
        pl.sum("ingresos_totales")).sort("ingresos_totales", descending=True)

    # Mostramos el gráfico de barras
    st.bar_chart(ventas_tienda.to_pandas(),
                 x="tienda_id", y="ingresos_totales")

    # --- FILTRO Y DETALLE ---
    st.divider()
    tienda_sel = st.selectbox(
        "Selecciona una tienda para ver detalle por categoría:", df["tienda_id"].unique().sort())
    detalle_tienda = df.filter(pl.col("tienda_id") == tienda_sel)
    st.dataframe(detalle_tienda.to_pandas(), width='stretch')
else:
    st.warning(
        "⚠️ Todavía no hay datos procesados en la capa Gold. Ejecuta el pipeline primero.")


st.divider()
st.subheader("💬 Últimas Reseñas del Market (Muestra Realista)")

# Datos para la simulación
productos = ["Leche Entera", "Detergente Pro",
             "Café Premium", "Manzanas Gala", "Pan Artesanal"]
opiniones_pos = [
    "Excelente calidad, volveré a comprar.",
    "Sabor auténtico y muy fresco.",
    "Relación calidad-precio inbatible."
]
opiniones_neg = [
    "El paquete llegó un poco golpeado.",
    "Esperaba algo más de cantidad por este precio.",
    "Sabor algo amargo para mi gusto, no lo recomiendo."
]

# Generar 10 comentarios aleatorios
for i in range(10):
    prod = random.choice(productos)
    # 70% de probabilidad de ser positivo, 30% negativo
    es_positivo = random.random() > 0.3

    texto = random.choice(opiniones_pos if es_positivo else opiniones_neg)
    icono = "✅" if es_positivo else "⚠️"
    color = "green" if es_positivo else "orange"

    # Diseño compacto: Usamos st.info para positivos y st.warning para negativos
    if es_positivo:
        st.success(f"**{prod}**: {texto}")
    else:
        st.warning(f"**{prod}**: {texto}")
