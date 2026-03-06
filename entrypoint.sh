#!/bin/bash

echo "Iniciando el Lakehouse..."

# 1. Generar datos iniciales
echo "Generando datos de ventas..."
python src/generator.py

# 2. Ejecutar el pipeline
echo "Procesando el pipeline (Bronze -> Gold)..."
python src/pipeline.py

# 3. Levantar Streamlit
echo "Levantando el Dashboard..."
streamlit run src/app.py