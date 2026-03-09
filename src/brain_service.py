# this script queries DuckDB to check sales
# Ollama generates reviews and comments based on those sales, and then saves those reviews back to the Bronze layer in Parquet format.

import os
import pandas as pd
import duckdb
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime

# 1. Configuración de Entorno e IA
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

api_key = os.getenv("GROQ_API_KEY")

if not api_key:
    print("❌ ERROR: brain_service no encuentra GROQ_API_KEY en el sistema")
else:
    print(f"✅ SUCCESS: brain_service detectó la clave: {api_key[:10]}...")

if api_key:
    client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=api_key)
    model_name = "llama3-8b-8192"
    print("🌐 Usando IA en la Nube (Groq)")
else:
    # host.docker.internal conecta el contenedor con tu Ollama de Windows
    client = OpenAI(
        base_url="http://ollama:11434/v1", api_key="ollama")
    model_name = "tinyllama"
    print("🏠 Usando Ollama Local")


def generar_opiniones():
    # 2. Conectar a DuckDB para obtener el contexto de ventas
    con = duckdb.connect('visor_lakehouse.db')

    try:
        # Leemos las ventas reales que generó generator.py
        top_ventas = con.execute("""
            SELECT producto, categoria, COUNT(*) as total 
            FROM read_parquet('data/bronze/ventas/*.parquet') 
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5
        """).df()
    except Exception as e:
        print(f"❌ Error: No se encontraron ventas en Bronze. {e}")
        return

    reviews_list = []

    # 3. Bucle de generación con la IA
    for _, row in top_ventas.iterrows():
        producto = row['producto']
        categoria = row['categoria']

        print(f"🤖 IA analizando: {producto}...")

        prompt = f"Actúa como un cliente de supermercado. El producto '{producto}' de la categoría '{categoria}' es muy popular. Escribe una reseña breve (máximo 15 palabras) sobre por qué te gusta."

        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}]
            )
            review_text = response.choices[0].message.content.strip()

            # Estructuramos el dato para el Lakehouse
            reviews_list.append({
                "fecha_review": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "producto": producto,
                "categoria": categoria,
                "review_cliente": review_text,
                "fuente": "AI_Generated"
            })
        except Exception as e:
            print(f"⚠️ Falló la generación para {producto}: {e}")

    # 4. Guardar como Parquet en la carpeta Bronze
    if reviews_list:
        df_reviews = pd.DataFrame(reviews_list)

        # Creamos la carpeta si no existe (importante para el primer arranque)
        os.makedirs('data/bronze/reviews', exist_ok=True)

        path_destino = 'data/bronze/reviews/reviews_ia.parquet'
        df_reviews.to_parquet(path_destino, index=False)

        print(
            f"✅ ¡Éxito! {len(reviews_list)} reseñas guardadas en {path_destino}")


if __name__ == "__main__":
    generar_opiniones()
