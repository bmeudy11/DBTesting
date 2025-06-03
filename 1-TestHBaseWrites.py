import happybase
import threading
import time
import random
import string
import uuid

# --- Configuration ---
HBASE_HOST = "localhost"  # Your HBase Thrift server host
HBASE_PORT = 9090         # Your HBase Thrift server port
TABLE_NAME = b'stress_test_table' # Table name as bytes
COLUMN_FAMILY = b'cf1'     # Column family as bytes
COLUMN_QUALIFIER = b'data' # Column qualifier as bytes

NUM_THREADS = 10           # Number of concurrent clients
ROWS_PER_THREAD = 1000     # Rows each client will put
ROW_KEY_PREFIX = "user_"
VALUE_SIZE_BYTES = 1024    # Approximate size of the value for each cell

# --- Helper Function to Generate Random Data ---
def generate_random_value(size_bytes):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(size_bytes)).encode('utf-8')

# --- Worker Function for Each Thread ---
def put_worker(thread_id, num_rows, table_name_bytes):
    connection = None
    try:
        # Each thread creates its own connection for true concurrency
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=30000) # 30s timeout
        table = connection.table(table_name_bytes)
        print(f"Thread {thread_id}: Starting to put {num_rows} rows.")

        for i in range(num_rows):
            # Generate a unique row key for each put
            row_key = f"{ROW_KEY_PREFIX}{thread_id}_{i}_{uuid.uuid4()}".encode('utf-8')
            value = generate_random_value(VALUE_SIZE_BYTES)
            data_to_put = {
                f'{COLUMN_FAMILY.decode("utf-8")}:{COLUMN_QUALIFIER.decode("utf-8")}': value
            }
            table.put(row_key, data_to_put)

            if (i + 1) % (num_rows // 10) == 0: # Log progress
                print(f"Thread {thread_id}: Put {i+1}/{num_rows} rows.")
        print(f"Thread {thread_id}: Finished putting rows.")

    except Exception as e:
        print(f"Thread {thread_id}: Error - {e}")
    finally:
        if connection:
            connection.close()
            # print(f"Thread {thread_id}: Connection closed.")

# --- Main Stress Test Logic ---
if __name__ == "__main__":
    start_time_main = time.time()
    connection = None
    try:
        # --- Setup: Connect and create table if it doesn't exist ---
        print(f"Connecting to HBase at {HBASE_HOST}:{HBASE_PORT} for setup...")
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=10000) # 10s timeout for setup
        connection.open()
        print("Connection opened for setup.")

        existing_tables = connection.tables()
        if TABLE_NAME in existing_tables:
            print(f"Table '{TABLE_NAME.decode('utf-8')}' already exists. Disabling and deleting for a fresh start.")
            connection.disable_table(TABLE_NAME)
            connection.delete_table(TABLE_NAME)
            print(f"Table '{TABLE_NAME.decode('utf-8')}' deleted.")

        print(f"Creating table '{TABLE_NAME.decode('utf-8')}' with column family '{COLUMN_FAMILY.decode('utf-8')}'...")
        families = {
            COLUMN_FAMILY.decode('utf-8'): dict() # Empty dict for default CF options
        }
        connection.create_table(TABLE_NAME, families)
        print(f"Table '{TABLE_NAME.decode('utf-8')}' created successfully.")

    except Exception as e:
        print(f"Setup error: {e}")
        if connection:
            connection.close()
        exit()
    finally:
        if connection:
            connection.close()
            print("Setup connection closed.")


    threads = []
    start_time_threads = time.time()

    print(f"\nStarting stress test with {NUM_THREADS} threads, {ROWS_PER_THREAD} puts/thread.")

    for i in range(NUM_THREADS):
        thread = threading.Thread(target=put_worker, args=(i, ROWS_PER_THREAD, TABLE_NAME))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time_threads = time.time()
    total_time_threads = end_time_threads - start_time_threads
    total_rows_put = NUM_THREADS * ROWS_PER_THREAD

    print("\n--- Stress Test Summary (Puts) ---")
    print(f"Total rows put: {total_rows_put}")
    print(f"Total time taken for puts: {total_time_threads:.2f} seconds")
    if total_time_threads > 0:
        print(f"Average puts per second: {total_rows_put / total_time_threads:.2f}")
    else:
        print("Total time for puts was too short to calculate puts per second accurately.")

    # Optional: You might want to verify data or clean up,
    # but for a pure write stress test, this might be skipped or done separately.
    # Example: count rows (can be slow on large tables)
    # connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
    # table = connection.table(TABLE_NAME)
    # row_count = 0
    # for _ in table.scan():
    #     row_count += 1
    # print(f"Verification: Total rows in table: {row_count}")
    # connection.close()