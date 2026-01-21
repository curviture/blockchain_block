import hashlib
import psycopg2
from datetime import datetime
from config import DB_CONFIG

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_block_header(block):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bitcoin_blocks (id, height, version, timestamp, tx_count, size, weight, merkle_root, difficulty)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING
        """, (
            block['id'], block['height'], block.get('version'),
            datetime.fromtimestamp(block['timestamp']), block['tx_count'],
            block['size'], block['weight'], block.get('merkle_root'),
            block.get('difficulty')
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def insert_transaction_batch(transactions, block_hash):
    if not transactions:
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for tx in transactions:
            # 1. Store Transaction Header
            status = tx.get('status', {})
            status_time = datetime.fromtimestamp(status.get('block_time')) if status.get('block_time') else None

            cur.execute("""
                INSERT INTO bitcoin_transactions (
                    txid, block_id, version, locktime, size, weight, fee,
                    status_confirmed, status_block_height, status_block_hash, status_block_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (txid) DO NOTHING
            """, (
                tx['txid'], block_hash, tx.get('version'), tx.get('locktime'),
                tx.get('size'), tx.get('weight'), tx.get('fee'),
                status.get('confirmed'), status.get('block_height'), 
                status.get('block_hash'), status_time
            ))

            # 2. Store Vouts
            for n, vout in enumerate(tx.get('vout', [])):
                cur.execute("""
                    INSERT INTO bitcoin_vouts (txid, vout_n, value, scriptpubkey_address, scriptpubkey_type)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (tx['txid'], n, vout.get('value'), vout.get('scriptpubkey_address'), vout.get('scriptpubkey_type')))

            # 3. Store Vins & Witnesses
            for n, vin in enumerate(tx.get('vin', [])):
                is_coinbase = vin.get('is_coinbase', False)
                prevout = vin.get('prevout') or {}
                cur.execute("""
                    INSERT INTO bitcoin_vins (txid, vin_n, is_coinbase, prevout_txid, prevout_vout_n, prevout_value, prevout_address)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (
                    tx['txid'], n, is_coinbase, 
                    vin.get('txid') if not is_coinbase else None, 
                    vin.get('vout') if not is_coinbase else None, 
                    prevout.get('value'), 
                    prevout.get('scriptpubkey_address')
                ))

                # Handle Witness Correlation (N-to-N)
                for stack_idx, item in enumerate(vin.get('witness', [])):
                    w_hash = hashlib.sha256(item.encode('utf-8')).hexdigest()
                    cur.execute("""
                        INSERT INTO bitcoin_witness_pool (witness_hash, witness_data) 
                        VALUES (%s, %s) ON CONFLICT (witness_hash) DO UPDATE SET witness_hash = EXCLUDED.witness_hash
                        RETURNING id
                    """, (w_hash, item))
                    witness_id = cur.fetchone()[0]
                    cur.execute("""
                        INSERT INTO vin_witness_correlation (txid, vin_n, stack_index, witness_id)
                        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """, (tx['txid'], n, stack_idx, witness_id))

        conn.commit()
        return len(transactions)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
