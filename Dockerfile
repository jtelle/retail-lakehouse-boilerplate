# 1. We use a lightweight Python image
FROM python:3.11-slim

# 2. We prevent Python from generating .pyc files and the buffer from filling up
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Working directory inside the container
WORKDIR /app

# 4. We install system dependencies if necessary
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 5. We copy and install your libraries (Polars, Streamlit, etc.)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. We copy the rest of your code
COPY . .

# Expose the Streamlit port just in case
EXPOSE 8501