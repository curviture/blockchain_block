import hashlib
import psycopg2
from datetime import datetime
from config import DB_CONFIG

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def is_block_fully_synced(block_hash, total_txs):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM bitcoin_transactions WHERE block_hash = %s", (block_hash,))
        count = cur.fetchone()[0]
        return count >= total_txs
    finally:
        cur.close()
        conn.close()

def insert_block_header(block):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bitcoin_blocks (
                block_hash, previous_block_hash, height, version, 
                merkle_root, timestamp, bits, nonce
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (block_hash) DO NOTHING
        """, (
            block['id'], block.get('previousblockhash'), block['height'], 
            block.get('version'), block.get('merkle_root'),
            block['timestamp'], block.get('bits'), block.get('nonce')
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def insert_transaction_batch(transactions, block_hash, base_index=0):
    if not transactions:
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for i, tx in enumerate(transactions):
            # Calculate absolute index in the block
            tx_index = base_index + i
            
            # Determine if it's a coinbase transaction
            is_coinbase = any(vin.get('is_coinbase', False) for vin in tx.get('vin', []))
            
            status = tx.get('status', {})

            cur.execute("""
                INSERT INTO bitcoin_transactions (
                    txid, block_hash, block_height, tx_index, version, locktime, is_coinbase
                ) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (txid) DO NOTHING
            """, (
                tx['txid'], block_hash, status.get('block_height'), tx_index,
                tx.get('version'), tx.get('locktime'), is_coinbase
            ))


            # 2. Store Outputs
            for n, vout in enumerate(tx.get('vout', [])):
                cur.execute("""
                    INSERT INTO bitcoin_outputs (
                        txid, output_index, value, script_pubkey, script_pubkey_asm,
                        script_pubkey_type, address
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (
                    tx['txid'], n, vout.get('value'), 
                    vout.get('scriptpubkey'), vout.get('scriptpubkey_asm'),
                    vout.get('scriptpubkey_type'), vout.get('scriptpubkey_address')
                ))


            # 3. Store Inputs (Level 3)
            for n, vin in enumerate(tx.get('vin', [])):
                cur.execute("""
                    INSERT INTO bitcoin_inputs (
                        txid, input_index, prev_txid, prev_vout, 
                        script_sig, script_sig_asm, sequence, is_coinbase
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (
                    tx['txid'], n, 
                    vin.get('txid'), vin.get('vout'),
                    vin.get('scriptsig'), vin.get('scriptsig_asm'),
                    vin.get('sequence'), vin.get('is_coinbase', False)
                ))

                # 4. Store Witness Data (if present)
                witness_items = vin.get('witness', [])
                for witness_idx, witness_data in enumerate(witness_items):
                    cur.execute("""
                        INSERT INTO bitcoin_witnesses (
                            txid, input_index, witness_index, witness_data
                        ) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """, (tx['txid'], n, witness_idx, witness_data))

        conn.commit()


        return len(transactions)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
