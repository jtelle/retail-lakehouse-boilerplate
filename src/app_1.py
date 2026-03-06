import streamlit as st
import polars as pl
import s3fs
import io
import plotly.express as px
import os

# Page configuration
st.set_page_config(page_title="Retail Lakehouse Dashboard", layout="wide")

st.title("📊 Retail Lakehouse - Capa Gold")
st.sidebar.header("Configuración")
if st.sidebar.button("🔄 Recargar datos de MinIO"):
    st.cache_data.clear()
    st.rerun()
st.markdown("Visualización en tiempo real de los datos procesados en **MinIO**.")

# 1. Connect to MinIO
minio_host = os.getenv("MINIO_ENDPOINT", "localhost:9000")
storage_options = {
    "key": "admin",
    "secret": "password123",
    "endpoint_url": f"http://{minio_host}",
    "client_kwargs": {"region_name": "us-east-1"}
}


@st.cache_data  # Cache to avoid reloading from S3 every time a button is pressed
def load_gold_data():
    fs = s3fs.S3FileSystem(**storage_options)
    gold_path = "lakehouse/gold/reporte_categorias.parquet"

    if not fs.exists(gold_path):
        return None

    with fs.open(gold_path, mode='rb') as f:
        # BytesIO to avoid path errors in Windows
        return pl.read_parquet(io.BytesIO(f.read()))


# 2. Load data from Gold layer
df_gold = load_gold_data()

if df_gold is not None:
    # --- MAIN METRICS ---
    col1, col2, col3 = st.columns(3)
    total_sales = df_gold["ingresos_totales"].sum()
    total_trans = df_gold["num_ventas"].sum()

    col1.metric("Ingresos Totales", f"{total_sales:,.2f} €")
    col2.metric("Nº Operaciones", f"{total_trans:,}")
    col3.metric("Categoría Top", df_gold["categoria"][0])

    # --- GRAPHS ---
    st.divider()
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Ventas por Categoría")
        fig_pie = px.pie(df_gold.to_pandas(),
                         values='ingresos_totales', names='categoria', hole=.3)
        st.plotly_chart(fig_pie, width='stretch')

    with c2:
        st.subheader("Eficiencia de Ventas")
        fig_bar = px.bar(df_gold.to_pandas(), x='categoria',
                         y='num_ventas', color='ingresos_totales')
        st.plotly_chart(fig_bar, width='stretch')

    # --- DATA TABLE ---
    st.subheader("Detalle de Capa Gold")
    st.dataframe(df_gold.to_pandas())  # , use_container_width=True)

else:
    st.error("⚠️ No se encontró el archivo en la Capa Gold. ¿Ejecutaste el pipeline?")
