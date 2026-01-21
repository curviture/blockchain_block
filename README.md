# Bitcoin Block Explorer & Indexer

A modular, high-performance Bitcoin block and transaction indexer with a built-in Flask web explorer. This project syncs data from the Blockstream.info API into a local PostgreSQL database using parallel processing for speed.

## 🚀 Features

- **Parallel Ingestion**: Fetches blockchain data in parallel batches using `ThreadPoolExecutor` for high performance.
- **Relational Data Model**: Fully normalized PostgreSQL schema for blocks, transactions, inputs (vins), outputs (vouts), and witness data.
- **Data Visualization**: A clean Flask-based web interface to explore blocks, transactions, and detailed script data.
- **Robust API Client**: Handles retries and timeouts gracefully to ensure data integrity during long syncs.

## 🛠️ Project Structure

- `dataFetch.py`: The main engine for orchestrating block and transaction syncing.
- `app.py`: Flask application to browse indexed blockchain data.
- `dbSetup.py`: SQL schema definitions and database initialization script.
- `db_operations.py`: CRUD operations for interacting with the PostgreSQL database.
- `api_client.py`: Efficient HTTP client optimized for the Blockstream API.
- `config.py`: Centralized configuration for database credentials and API settings.

## 📦 Setup & Installation

### 1. Prerequisites
- Python 3.x
- PostgreSQL database
- `psycopg2`, `flask`, `requests` libraries

### 2. Configure Database
Update `config.py` with your PostgreSQL credentials:
```python
DB_CONFIG = {
    'dbname': 'your_db_name',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}
```

### 3. Initialize Schema
Run the setup script to create the necessary tables:
```bash
python3 dbSetup.py
```

### 4. Sync Data
Start the ingestion process to fetch the latest blocks:
```bash
python3 dataFetch.py
```

### 5. Start the Web Explorer
Run the Flask app to view the data in your browser:
```bash
python3 app.py
```
Navigate to `http://127.0.0.1:5000` to start exploring.

## 📄 License
MIT
