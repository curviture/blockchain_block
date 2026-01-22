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
        cur.execute("DROP TABLE IF EXISTS bitcoin_witnesses CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_inputs CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bitcoin_outputs CASCADE;")

        cur.execute("DROP TABLE IF EXISTS bitcoin_transactions CASCADE;")

        cur.execute("DROP TABLE IF EXISTS bitcoin_blocks CASCADE;")
        
        # 1. Blocks Table (Level 1)
        print("Creating table: bitcoin_blocks")
        cur.execute("""
            CREATE TABLE bitcoin_blocks (
                block_hash VARCHAR(64) PRIMARY KEY,
                previous_block_hash VARCHAR(64),
                height INTEGER UNIQUE NOT NULL,
                version BIGINT,
                merkle_root VARCHAR(64),
                timestamp BIGINT NOT NULL,
                bits VARCHAR(64),
                nonce BIGINT
            );
        """)
        
        # 2. Transactions Table (Level 2)
        print("Creating table: bitcoin_transactions")
        cur.execute("""
            CREATE TABLE bitcoin_transactions (
                txid VARCHAR(64) PRIMARY KEY,
                block_hash VARCHAR(64) REFERENCES bitcoin_blocks(block_hash) ON DELETE CASCADE,
                block_height INTEGER,
                tx_index INTEGER,
                version INTEGER,
                locktime BIGINT,
                is_coinbase BOOLEAN
            );
        """)



        # 3. Outputs Table (Detail of Transaction - "Money Created")
        print("Creating table: bitcoin_outputs")
        cur.execute("""
            CREATE TABLE bitcoin_outputs (
                txid VARCHAR(64) REFERENCES bitcoin_transactions(txid) ON DELETE CASCADE,
                output_index INTEGER,
                value BIGINT,
                script_pubkey TEXT,
                script_pubkey_asm TEXT,
                script_pubkey_type VARCHAR(50),
                address VARCHAR(100),
                PRIMARY KEY (txid, output_index)
            );
        """)


        # 4. Inputs Table (Detail of Transaction - "Money Spent")
        print("Creating table: bitcoin_inputs")
        cur.execute("""
            CREATE TABLE bitcoin_inputs (
                txid VARCHAR(64) REFERENCES bitcoin_transactions(txid) ON DELETE CASCADE,
                input_index INTEGER,
                prev_txid VARCHAR(64),
                prev_vout BIGINT,
                script_sig TEXT,
                script_sig_asm TEXT,
                sequence BIGINT,
                is_coinbase BOOLEAN,
                PRIMARY KEY (txid, input_index)
            );
        """)

        # 5. Witnesses Table (SegWit witness data)
        print("Creating table: bitcoin_witnesses")
        cur.execute("""
            CREATE TABLE bitcoin_witnesses (
                txid VARCHAR(64),
                input_index INTEGER,
                witness_index INTEGER,
                witness_data TEXT,
                PRIMARY KEY (txid, input_index, witness_index),
                FOREIGN KEY (txid, input_index) REFERENCES bitcoin_inputs(txid, input_index) ON DELETE CASCADE
            );
        """)

        # 6. Aggregated View
        print("Creating view: block_stats_view")
        cur.execute("""
            CREATE OR REPLACE VIEW block_stats_view AS
            SELECT 
                b.height,
                b.block_hash,
                b.timestamp,
                COUNT(DISTINCT t.txid) AS transaction_count,
                COALESCE(SUM(o.value), 0) AS total_volume_sats,
                (COALESCE(SUM(o.value), 0) / 100000000.0) AS total_volume_btc
            FROM 
                bitcoin_blocks b
            LEFT JOIN 
                bitcoin_transactions t ON b.block_hash = t.block_hash
            LEFT JOIN 
                bitcoin_outputs o ON t.txid = o.txid
            GROUP BY 
                b.height, b.block_hash, b.timestamp;
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Full Relational Blockchain Schema is ready!")
    except Exception as e:
        print(f"❌ Database setup failed: {e}")

if __name__ == "__main__":
    setup_database()
