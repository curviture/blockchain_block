import psycopg2

from config import DB_CONFIG

def setup_database():
    """Build the full relational schema: Blocks -> Transactions -> (Vins & Vouts)."""
    print("Rebuilding database schema with Vin/Vout support...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # We start by dropping in reverse order of dependencies
        print("Cleaning up old tables...")
        cur.execute("DROP TABLE IF EXISTS vin_witness_correlation CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_witness_pool CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_vins CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_vouts CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_transactions CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_blocks CASCADE;")
        
        # 1. Blocks Table (Level 1)
        print("Creating table: bitcoin_blocks")
        cur.execute("""
            CREATE TABLE bitcoin_blocks (
                id VARCHAR(64) PRIMARY KEY,
                height INTEGER UNIQUE NOT NULL,
                version BIGINT,
                timestamp TIMESTAMP NOT NULL,
                tx_count INTEGER,
                size INTEGER,
                weight INTEGER,
                merkle_root VARCHAR(64),
                difficulty NUMERIC
            );
        """)
        
        # 2. Transactions Table (Level 2)
        print("Creating table: bitcoin_transactions")
        cur.execute("""
            CREATE TABLE bitcoin_transactions (
                txid VARCHAR(64) PRIMARY KEY,
                block_id VARCHAR(64) REFERENCES bitcoin_blocks(id) ON DELETE CASCADE,
                version INTEGER,
                locktime BIGINT,
                size INTEGER,
                weight INTEGER,
                fee BIGINT,
                status_confirmed BOOLEAN,
                status_block_height INTEGER,
                status_block_hash VARCHAR(64),
                status_block_time TIMESTAMP
            );
        """)

        # 3. Vouts Table (Detail of Transaction - "Money Created")
        print("Creating table: bitcoin_vouts")
        cur.execute("""
            CREATE TABLE bitcoin_vouts (
                txid VARCHAR(64) REFERENCES bitcoin_transactions(txid) ON DELETE CASCADE,
                vout_n INTEGER,
                value BIGINT,
                scriptpubkey_address VARCHAR(100),
                scriptpubkey_type VARCHAR(50),
                PRIMARY KEY (txid, vout_n)
            );
        """)

        # 4. Vins Table (Detail of Transaction - "Money Spent")
        print("Creating table: bitcoin_vins")
        cur.execute("""
            CREATE TABLE bitcoin_vins (
                txid VARCHAR(64) REFERENCES bitcoin_transactions(txid) ON DELETE CASCADE,
                vin_n INTEGER,
                is_coinbase BOOLEAN,
                prevout_txid VARCHAR(64),
                prevout_vout_n INTEGER,
                prevout_value BIGINT,
                prevout_address VARCHAR(100),
                PRIMARY KEY (txid, vin_n)
            );
        """)

        # 5. Witness Data Pool (Unique strings only)
        print("Creating table: bitcoin_witness_pool")
        cur.execute("""
            CREATE TABLE bitcoin_witness_pool (
                id SERIAL PRIMARY KEY,
                witness_hash CHAR(64) UNIQUE NOT NULL,
                witness_data TEXT NOT NULL
            );
        """)

        # 6. Vin-Witness Correlation Table (The Junction)
        print("Creating table: vin_witness_correlation")
        cur.execute("""
            CREATE TABLE vin_witness_correlation (
                txid VARCHAR(64),
                vin_n INTEGER,
                stack_index INTEGER,
                witness_id INTEGER REFERENCES bitcoin_witness_pool(id),
                PRIMARY KEY (txid, vin_n, stack_index),
                FOREIGN KEY (txid, vin_n) REFERENCES bitcoin_vins(txid, vin_n) ON DELETE CASCADE
            );
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Full Relational Blockchain Schema is ready!")
    except Exception as e:
        print(f"❌ Database setup failed: {e}")

if __name__ == "__main__":
    setup_database()
