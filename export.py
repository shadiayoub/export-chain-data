import json
import requests
import logging
from time import sleep
from tqdm import tqdm  # For progress bar

# Configuration
RPC_URL = "http://localhost:21264"  # Change if needed
EXPORT_FILE = "blockchain_partial.rlp"
END_BLOCK = 555555  # Change this to the last block you want to export
RETRY_COUNT = 3  # Number of retries for failed requests
RETRY_DELAY = 2  # Delay between retries in seconds

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_block_rlp(block_number):
    """Fetch block RLP from Geth using debug_getBlockRlp."""
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_getBlockRlp",
        "params": [block_number],  # Pass block number as an integer
        "id": block_number
    }
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.post(RPC_URL, json=payload)
            response.raise_for_status()  # Raise an error for bad status codes
            data = response.json()

            logger.debug(f"Block {block_number} response: {json.dumps(data, indent=2)}")  # DEBUG

            if "result" in data and isinstance(data["result"], str):
                return data["result"]
            else:
                logger.warning(f"Block {block_number} RLP not found")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt + 1} failed for block {block_number}: {e}")
            if attempt < RETRY_COUNT - 1:
                sleep(RETRY_DELAY)
            else:
                logger.error(f"Failed to fetch block {block_number} after {RETRY_COUNT} attempts")
                return None

def export_blocks(start_block=0, end_block=END_BLOCK):
    """Export blocks from start_block to end_block into an RLP file."""
    try:
        with open(EXPORT_FILE, "wb") as f:
            for block_number in tqdm(range(start_block, end_block + 1), desc="Exporting blocks"):
                logger.info(f"Exporting block {block_number}...")
                rlp_data = get_block_rlp(block_number)

                if rlp_data:
                    try:
                        f.write(bytes.fromhex(rlp_data[2:]))  # Remove "0x" and convert to bytes
                    except Exception as e:
                        logger.error(f"Error writing block {block_number}: {e}")
                else:
                    logger.warning(f"Skipping block {block_number}")

        logger.info(f"Export completed: {EXPORT_FILE}")

    except Exception as e:
        logger.error(f"An error occurred during export: {e}")

if __name__ == "__main__":
    export_blocks()
