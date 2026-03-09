#!/bin/bash

#!/bin/bash
echo "🚀 Iniciando el Lakehouse..."

# 1. Generar datos iniciales (Ventas)
python src/generator.py

# 2. ESPERAR A OLLAMA
# No podemos generar reseñas si el modelo no ha bajado
if [ -n "$GROQ_API_KEY" ]; then
    echo "✅ Groq detectado. Saltando espera de Ollama..."
else
    echo "⏳ Esperando a que el modelo de IA local esté listo..."
    until curl -s http://ollama:11434/api/tags | grep -q "tinyllama"; do
      sleep 5
    done
    echo "✅ IA local lista."
fi
echo "✅ IA lista. Generando reseñas..."

# 3. Generar reseñas con la IA
python src/brain_service.py

# 4. Ejecutar el pipeline (Bronze -> Gold)
# Ahora el pipeline procesará ventas Y reseñas
python src/pipeline.py

# 5. Levantar Streamlit
echo "📊 Levantando el Dashboard..."
# Esta línea DEBE ser la última y no debe tener un "&" al final
# Usamos 0.0.0.0 para que Streamlit acepte conexiones desde fuera del contenedor
streamlit run src/app.py --server.port=8501 --server.address=0.0.0.0