import psycopg2
from config import DB_CONFIG
from datetime import datetime

def create_view(cur):
    """Creates or replaces the aggregated statistics view."""
    print("🔨 Building SQL View: block_stats_view...")
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
            b.height, b.block_hash, b.timestamp
        ORDER BY 
            b.height DESC;
    """)

def format_table(rows, headers):
    """Formats data as a clean ASCII table."""
    if not rows:
        return "No data found."

    # Convert timestamps to readable dates and format numbers
    formatted_rows = []
    for row in rows:
        r = list(row)
        # Format Timestamp (Index 2)
        r[2] = datetime.fromtimestamp(r[2]).strftime('%Y-%m-%d %H:%M:%S')
        # Truncate Hash (Index 1) for better display
        r[1] = r[1][:8] + "..." + r[1][-8:]
        # Format Volume BTC (Index 5) to 8 decimals
        r[5] = f"{r[5]:.8f}"
        formatted_rows.append([str(item) for item in r])

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in formatted_rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    # Build DB Table string
    separator = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    header_row = "|" + "|".join(f" {h:<{w}} " for h, w in zip(headers, col_widths)) + "|"
    
    table_str = [separator, header_row, separator]
    
    for row in formatted_rows:
        data_row = "|" + "|".join(f" {cell:<{w}} " for cell, w in zip(row, col_widths)) + "|"
        table_str.append(data_row)
    
    table_str.append(separator)
    return "\n".join(table_str)

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1. Ensure the view exists
        create_view(cur)
        conn.commit()

        # 2. Query the data
        print("🔍 Querying Aggregated Data...")
        cur.execute("SELECT * FROM block_stats_view")
        rows = cur.fetchall()
        
        # Get column names
        headers = [desc[0].upper() for desc in cur.description] # ['HEIGHT', 'BLOCK_HASH', ...]

        # 3. Display
        print("\n📊 BLOCKCHAIN AGGREGATED STATS")
        print(format_table(rows, headers))
        print(f"\nTotal Blocks: {len(rows)}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
