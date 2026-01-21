from flask import Flask, render_template, abort
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG

app = Flask(__name__)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.route('/')
def index():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM bitcoin_blocks ORDER BY height DESC;")
        blocks = cur.fetchall()
        cur.close(); conn.close()
        return render_template('index.html', blocks=blocks)
    except Exception as e:
        return str(e), 500

@app.route('/block/<block_hash>')
def block_details(block_hash):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM bitcoin_blocks WHERE id = %s;", (block_hash,))
        block = cur.fetchone()
        if not block: abort(404)
        
        cur.execute("SELECT * FROM bitcoin_transactions WHERE block_id = %s ORDER BY fee DESC;", (block_hash,))
        transactions = cur.fetchall()
        cur.close(); conn.close()
        return render_template('block_details.html', block=block, transactions=transactions)
    except Exception as e:
        return str(e), 500

@app.route('/tx/<txid>')
def transaction_details(txid):
    """View details of a single transaction including Vins and Vouts."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Fetch transaction header
        cur.execute("SELECT * FROM bitcoin_transactions WHERE txid = %s;", (txid,))
        tx = cur.fetchone()
        if not tx: abort(404)
        
        # 2. Fetch Vouts (Outputs)
        cur.execute("SELECT * FROM bitcoin_vouts WHERE txid = %s ORDER BY vout_n;", (txid,))
        vouts = cur.fetchall()
        
        # 3. Fetch Vins (Inputs)
        cur.execute("SELECT * FROM bitcoin_vins WHERE txid = %s ORDER BY vin_n;", (txid,))
        vins = cur.fetchall()

        # 4. Fetch Witness Items (The N-to-N Correlation)
        cur.execute("""
            SELECT c.vin_n, p.witness_data, c.stack_index
            FROM vin_witness_correlation c
            JOIN bitcoin_witness_pool p ON c.witness_id = p.id
            WHERE c.txid = %s
            ORDER BY c.vin_n, c.stack_index;
        """, (txid,))
        witness_rows = cur.fetchall()
        
        # Group witnesses by vin_n for easy access in template
        witnesses = {}
        for row in witness_rows:
            if row['vin_n'] not in witnesses:
                witnesses[row['vin_n']] = []
            witnesses[row['vin_n']].append(row['witness_data'])
        
        cur.close(); conn.close()
        return render_template('transaction_details.html', tx=tx, vouts=vouts, vins=vins, witnesses=witnesses)
    except Exception as e:
        return str(e), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
