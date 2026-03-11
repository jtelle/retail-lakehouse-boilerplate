# 🛒 Modern Retail Lakehouse

This project demonstrates a fully functional **Modern Data Stack** (MDS) architecture containerized with Docker. It simulates a retail business with 50 stores, processing data through Bronze and Gold layers with an integrated AI-driven review engine.

## Key Features
- **High Performance:** Powered by Polars and DuckDB for fast queries.
- **Medallion Architecture:** Clear separation between Bronze (Raw) and Gold (Reporting) layers.
- **Integrated Orchestration:** Trigger the entire generation and transformation pipeline with one click from the UI.
- **Smart AI Engine:** Automated customer review generation using Groq (Cloud) or Ollama (Local).
- **Full Containerization:** One-command setup with Docker Compose.

## Architecture
- **Storage:** MinIO (S3-compatible Object Storage)
- **Format:** Apache Parquet (Columnar storage)
- **Compute:** Polars (High-performance DataFrames) & DuckDB (OLAP SQL engine)
- **Orchestration:** Docker Compose
- **Visualization:** Streamlit Dashboard

 

## Features
- **Medallion Architecture:** Raw data (Bronze) to Aggregated reports (Gold).
- **Schema-on-Read:** Querying Parquet files directly from S3 using DuckDB.
- **Persistence:** Docker volumes ensure data survives container restarts.

## How to Run

Follow these steps to get the Lakehouse up and running on your local machine.

### 1. Prerequisites

Ensure you have Docker and Docker Compose installed:
- [Install Docker](https://docs.docker.com/get-docker/)

### 2. Launch the Infrastructure

Clone the repository and start the containers in detached mode:
```bash
git clone <your-repo-url>
cd <your-repo-name>
docker-compose up -d
```

### 3. Access the Services

The environment exposes several endpoints for interaction:

| Tool               | URL                       | Credentials         |
|--------------------|---------------------------|---------------------|
| Streamlit Dashboard| http://localhost:8502     | None                |
| MinIO Console      | http://localhost:9001     | admin / password123 |
| Ollama API         | http://localhost:11434    | None                |

### 4. Update the Lakehouse (Self-Service)

The `entrypoint.sh` script automatically initializes the data on startup. To generate new data and update the reports:

1. Open the Streamlit Dashboard.
2. Expand the **Data Control Panel**.
3. Click **INICIAR PROCESO COMPLETO**. The UI will show a real-time log of the generation, AI analysis, and pipeline transformation.
4. Click **VER GRÁFICAS ACTUALIZADAS** to see the new results.

### 5. Smart AI Engine (Hybrid Mode)

The processing engine automatically detects your environment during startup:

- **High‑Performance Mode (Cloud):** If a `GROQ_API_KEY` is found in your `.env` file, the system uses Groq for near-instant inference.
- **Local Fallback Mode:** If no key is provided, the system waits for the local Ollama service to be ready and uses the `tinyllama` model.

### 6. Advanced: Manual Querying (SQL)

Since this is a Schema-on-Read architecture, you can explore the data directly using SQL:

1. Connect DBeaver: Create a new DuckDB connection pointing to `visor_lakehouse.db` in your project folder.
2. Initialize S3 Access: Execute the first section of `src/queries_maestras.sql` to configure the S3/MinIO connection.
3. Create Views: Run the `CREATE VIEW` statements to map the Parquet files in MinIO as virtual SQL tables.

---

## 📁 Project Structure

```
├── src/
│   ├── app.py            # Streamlit Dashboard logic
│   ├── generator.py      # Synthetic data engine (Boto3/Polars)
│   ├── brain_service.py # AI Review generator (Groq/Ollama)
│   ├── pipeline.py       # Bronze to Gold processing logic
├── sql/
│   └── queries_maestras.sql
├── docker-compose.yml
├── requirements.txt
└── README.md
```

#### Ports map


| Service       | Port | Description                   |
|---------------|------|-------------------------------|
| Streamlit UI  | 8502 | Main Data Dashboard           |
| MinIO Console | 9001 | Object Storage Management     |
| Ollama API    | 11435| LLM Engine (Mapped from 11434)|


