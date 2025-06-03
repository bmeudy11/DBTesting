import happybase
import threading
import time
import random
import string
import uuid

# --- Configuration ---
HBASE_HOST = "localhost"
HBASE_PORT = 9090
TABLE_NAME = b'stress_test_table_pool'
COLUMN_FAMILY = b'cf1'
NUM_THREADS = 20 # Can be higher with a pool
OPERATIONS_PER_THREAD = 500
DATA_SIZE_BYTES = 128
POOL_SIZE = 10 # Size of the connection pool

# Operation mix
PROB_PUT = 0.6
PROB_GET = 0.4

# --- Global Connection Pool ---
# Initialize this once
hbase_pool = None

def initialize_hbase_pool():
    global hbase_pool
    if hbase_pool is None:
        print(f"Initializing HBase connection pool with size {POOL_SIZE}...")
        hbase_pool = happybase.ConnectionPool(size=POOL_SIZE, host=HBASE_HOST, port=HBASE_PORT, timeout=20000)
        # You might want to add other connection parameters like transport, protocol if needed

# --- Helper Functions (same as before) ---
def generate_random_data(size_bytes):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(size_bytes)).encode('utf-8')

def generate_row_key(prefix="key"):
    return f"{prefix}_{uuid.uuid4()}".encode('utf-8')


# --- Worker Function using Connection Pool ---
def pool_worker(thread_id, num_ops):
    ops_done = {"put": 0, "get_found": 0, "get_not_found": 0, "error": 0}
    # No local known_keys needed if we don't share state between ops in this simple worker
    # For more complex scenarios, you might still need a way to get valid keys for GETs

    for i in range(num_ops):
        try:
            with hbase_pool.connection() as connection: # Get connection from pool
                table = connection.table(TABLE_NAME)
                op_choice = random.random()

                if op_choice < PROB_PUT:
                    row_key = generate_row_key(prefix=f"pool_thr{thread_id}")
                    data_col = generate_random_data(DATA_SIZE_BYTES)
                    data_to_put = {f'{COLUMN_FAMILY.decode()}:data'.encode('utf-8'): data_col}
                    table.put(row_key, data_to_put)
                    ops_done["put"] += 1
                else:
                    # GET - for this to be effective, we need keys.
                    # Simplistic: try to get a recently generated-style key.
                    # A better approach would be to have a shared list of recently put keys.
                    # Or query based on known patterns/prefixes if your data has them.
                    # For this example, let's assume we are mostly testing puts,
                    # and gets might often miss unless the key space is small or targeted.
                    # Let's try a Get on a *potentially* existing key format
                    # This part needs a better strategy for key selection in a real test.
                    
                    # Construct a plausible key (this is a weak point without shared state of written keys)
                    random_thread_for_key = random.randint(0, NUM_THREADS-1)
                    random_op_for_key = random.randint(0, i) # try to get a key from earlier in this thread (not robust)
                    potential_key = f"pool_thr{random_thread_for_key}_{random_op_for_key}".encode('utf-8') # This is very unlikely to hit unless keys are very predictable.
                                        
                    # A slightly better approach: pick a random prefix and get one.
                    # This still depends on data existing for that prefix.
                    prefix_to_get = f"pool_thr{random.randint(0, NUM_THREADS-1)}".encode('utf-8')
                    found_row = None
                    for key, data in table.scan(row_prefix=prefix_to_get, limit=1):
                        found_row = data # Get the data of the first row found with this prefix
                        break # only need one for this example GET

                    if found_row:
                        ops_done["get_found"] += 1
                    else:
                        ops_done["get_not_found"] += 1
            
            if (i + 1) % (num_ops // 10 or 1) == 0:
                print(f"Thread {thread_id}: Completed {i+1}/{num_ops} ops via pool. Stats: {ops_done}")

        except Exception as e:
            print(f"Thread {thread_id}: Pool Worker Error - {e}")
            ops_done["error"] += 1
    
    print(f"Thread {thread_id}: Pool Worker Finished. Final Ops: {ops_done}")
    return ops_done

# --- Main Stress Test Logic ---
if __name__ == "__main__":
    initialize_hbase_pool() # Initialize the pool once

    # --- Setup Table ---
    try:
        with hbase_pool.connection() as connection:
            if TABLE_NAME not in connection.tables():
                print(f"Creating table: {TABLE_NAME.decode()} with CF: {COLUMN_FAMILY.decode()}")
                connection.create_table(TABLE_NAME, {COLUMN_FAMILY: dict()})
            else:
                print(f"Table {TABLE_NAME.decode()} already exists.")
                # Optionally clear table for a fresh test
                # admin = connection.client # HappyBase Admin client is not directly exposed this way
                # For table admin operations with pool, you might need a separate non-pooled connection or handle it outside
    except Exception as e:
        print(f"HBase setup error with pool: {e}")
        exit()


    threads = []
    all_ops_summary = {"put": 0, "get_found": 0, "get_not_found": 0, "error": 0}
    start_time = time.time()

    print(f"Starting Pool-based stress test with {NUM_THREADS} threads, {OPERATIONS_PER_THREAD} ops/thread.")
    
    thread_results = []
    for i in range(NUM_THREADS):
        thread = threading.Thread(target=lambda: thread_results.append(pool_worker(i, OPERATIONS_PER_THREAD)))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()

    # Aggregate results
    for res in thread_results:
        if res:
            for key in all_ops_summary:
                all_ops_summary[key] += res.get(key, 0)

    total_time = end_time - start_time
    total_operations_attempted = NUM_THREADS * OPERATIONS_PER_THREAD
    total_operations_completed = sum(all_ops_summary.values()) - all_ops_summary["error"]

    print("\n--- Pool-based Stress Test Summary ---")
    print(f"Total operations attempted: {total_operations_attempted}")
    print(f"Total operations completed successfully: {total_operations_completed}")
    print(f"Detailed breakdown: {all_ops_summary}")
    print(f"Total time taken: {total_time:.2f} seconds")
    if total_time > 0 and total_operations_completed > 0:
        print(f"Average successful operations per second: {total_operations_completed / total_time:.2f}")
    else:
        print("Not enough successful operations or time to calculate ops/sec accurately.")

    # The pool itself does not need explicit closing in this script structure,
    # as connections are returned. If the pool object itself had a close method,
    # you'd call it at the very end of the application. HappyBase's pool
    # manages connections internally.