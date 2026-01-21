import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from api_client import get_api_data
from db_operations import insert_block_header, insert_transaction_batch

def sync_full_block(block):
    """Orchestrates parallel fetching and storage of all transactions in a block."""
    block_hash = block['id']
    total_txs = block['tx_count']
    print(f"\n📦 Block #{block['height']}: Syncing ALL {total_txs} transactions...")

    # 1. Store Header
    insert_block_header(block)

    # 2. Setup Pagination
    indices = list(range(0, total_txs, 25))
    total_stored = 0

    # 3. Parallel Batch Processing (Bumped to 12 workers for speed)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for idx in indices:
            url = f"https://blockstream.info/api/block/{block_hash}/txs/{idx}"
            futures[executor.submit(get_api_data, url)] = idx
        
        for future in as_completed(futures):
            tx_data = future.result()
            if tx_data:
                try:
                    count = insert_transaction_batch(tx_data, block_hash)
                    total_stored += count
                    # Clearer progress display
                    print(f"   ∟ Progress: {total_stored}/{total_txs} transactions indexed...", end='\r')
                except Exception as e:
                    print(f"\n   ❌ Batch store failed: {e}")

    print(f"\n✅ Block #{block['height']} fully indexed.")

def main():
    print("🚀 Starting Modular Parallel Ingestion...")
    # Fetch latest 10 blocks
    blocks = get_api_data("https://blockstream.info/api/blocks")
    
    if blocks:
        for block in blocks:
            sync_full_block(block)
            # Short rest between blocks to keep pool healthy
            time.sleep(1)
        print("\n🎉 ALL DONE: Your relational database is fully synced.")

if __name__ == "__main__":
    main()
