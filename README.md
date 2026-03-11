# 🛒 Modern Retail Lakehouse

This project demonstrates a fully functional **Modern Data Stack** (MDS) architecture containerized with Docker. It simulates a retail business with 50 stores, processing data through Bronze and Gold layers.

## Key Features
- **High Performance:** Powered by Polars and DuckDB for lightning-fast OLAP queries.
- **Medallion Architecture:** Clear separation between Bronze (Raw) and Gold (Reporting) layers.
- **Full Containerization:** One-command setup with Docker Compose.
- **Live Dashboard:** Interactive Streamlit UI connected directly to the Lakehouse.

## Architecture
- **Storage:** MinIO (S3-compatible Object Storage)
- **Format:** Apache Parquet (Columnar storage)
- **Compute:** Polars (High-performance DataFrames) & DuckDB (OLAP SQL engine)
- **Orchestration:** Docker Compose
- **Visualization:** Streamlit Dashboard

## Quick Start
1. `docker-compose up -d`, access the Dashboard at `http://localhost:8502`
 - This will run the whole pipeline. To run again:
    - Run data generation: `docker exec -it lakehouse_ui python src/generator.py`
    - Process the pipeline: `docker exec -it lakehouse_ui python src/pipeline.py`
    

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

### 3. Initialize the Lakehouse
->Skip this step with new entrypoint.sh, that will run all the scripts.

Execute the internal scripts to create the storage buckets, generate synthetic sales data for 50 stores, and process the Medallion pipeline:
```bash
# Generate raw sales data (Bronze Layer)
docker exec -it lakehouse_ui python src/generator.py

# Process data and create aggregates (Gold Layer)
docker exec -it lakehouse_ui python src/pipeline.py
```

### 4. Access the Tools

The environment exposes several endpoints for interaction:

| Tool                 | URL                                | Credentials           |
|----------------------|------------------------------------|-----------------------|
| Streamlit Dashboard  | http://localhost:8502              | None                  |
| MinIO Console        | http://localhost:9001              | admin / password123   |
| DBeaver (DuckDB)     | path/to/project/visor_lakehouse.db	| Standard Connection   |

### 5 Smart AI Engine (Hybrid Mode)

This Lakehouse is designed for maximum flexibility. The processing engine automatically detects your environment during startup:

* **High-Performance Mode (Cloud):** If a `GROQ_API_KEY` is found in your `.env` file, the system bypasses local checks and uses Groq for near-instant inference.
* **Local Fallback Mode:** If no API key is provided, the `entrypoint.sh` script automatically waits for the local **Ollama** service to be ready with the `tinyllama` model.

### 6. Querying with SQL (First-time Setup)

Since this is a Schema-on-Read architecture, follow these steps to explore the data:

 - Connect DBeaver: Create a new DuckDB connection pointing to visor_lakehouse.db in your project folder (DBeaver will create the file if it doesn't exist).

- Initialize S3 Access: Open and execute the first section of src/queries_maestras.sql. This tells DuckDB how to talk to the MinIO container.

- Create Views: Run the CREATE VIEW statements in the same script. This "maps" the Parquet files in S3 as SQL tables.

---

## 📁 Project Structure

```
├── src/
│   ├── app.py            # Streamlit Dashboard logic
│   ├── generator.py      # Synthetic data engine (Boto3/Polars)
│   ├── pipeline.py       # Bronze to Gold processing logic
├── sql/
│   └── queries_maestras.sql
├── docker-compose.yml
├── requirements.txt
└── README.md
```

#### Ports map


| Service       | Port | Description              |
|---------------|------|--------------------------|
| Streamlit UI  | 8502 | Main Data Dashboard      |
| MinIO Console | 9001 | Object Storage Management|
| Ollama API    | 11435| Local LLM Engine          |


