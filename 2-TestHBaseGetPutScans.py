import happybase
import threading
import time
import random
import string
import uuid

# --- Configuration ---
HBASE_HOST = "localhost"
HBASE_PORT = 9090
TABLE_NAME = b'stress_test_table_mixed'
COLUMN_FAMILY = b'cf1' # Ensure this column family exists

NUM_THREADS = 10
OPERATIONS_PER_THREAD = 200
DATA_SIZE_BYTES = 128

# Operation mix (probabilities should sum to 1.0)
PROB_PUT = 0.50
PROB_GET = 0.35
PROB_SCAN = 0.15 # Scans can be expensive; adjust probability

# Store some row keys of inserted data for Gets/Scans.
# This needs careful handling in a multithreaded environment.
# For simplicity, each thread can try to read keys it knows it inserted,
# or we can pre-populate and have threads pick randomly.
# Using a thread-safe list for shared row keys:
known_row_keys_lock = threading.Lock()
known_row_keys = [] # Populated by Puts, used by Gets/Scans

# --- Helper Functions ---
def generate_random_data(size_bytes):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(size_bytes)).encode('utf-8')

def generate_row_key(prefix="key"):
    return f"{prefix}_{uuid.uuid4()}".encode('utf-8')

# --- Worker Function ---
def mixed_worker(thread_id, num_ops, initial_keys):
    connection = None
    ops_done = {"put": 0, "get_found": 0, "get_not_found": 0, "scan": 0, "error": 0}
    local_inserted_keys = list(initial_keys) # Each thread gets a copy of initial keys

    try:
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=30000)
        table = connection.table(TABLE_NAME)
        print(f"Thread {thread_id}: Starting {num_ops} mixed operations.")

        for i in range(num_ops):
            op_choice = random.random()
            try:
                if op_choice < PROB_PUT:
                    # PUT
                    row_key = generate_row_key(prefix=f"thr{thread_id}")
                    data_col = generate_random_data(DATA_SIZE_BYTES)
                    data_to_put = {f'{COLUMN_FAMILY.decode()}:data'.encode('utf-8'): data_col}
                    table.put(row_key, data_to_put)
                    with known_row_keys_lock:
                        if len(known_row_keys) < NUM_THREADS * OPERATIONS_PER_THREAD * 0.1 : # Cap size
                            known_row_keys.append(row_key)
                    local_inserted_keys.append(row_key)
                    ops_done["put"] += 1

                elif op_choice < PROB_PUT + PROB_GET:
                    # GET
                    target_key = None
                    if local_inserted_keys and random.random() < 0.7: # Prefer local
                        target_key = random.choice(local_inserted_keys)
                    elif known_row_keys: # Fallback to global
                        with known_row_keys_lock:
                            if known_row_keys: # Re-check after acquiring lock
                                target_key = random.choice(known_row_keys)
                    
                    if target_key:
                        row_data = table.row(target_key)
                        if row_data:
                            ops_done["get_found"] += 1
                        else:
                            ops_done["get_not_found"] += 1
                    else: # No key to get
                        ops_done["get_not_found"] += 1


                elif op_choice < PROB_PUT + PROB_GET + PROB_SCAN:
                    # SCAN
                    # Simple scan: get a few rows starting from a known or random prefix
                    start_row_prefix = None
                    if local_inserted_keys and random.random() < 0.5:
                        start_row_prefix = local_inserted_keys[0][:10] # Use a prefix of an existing key
                    elif known_row_keys:
                         with known_row_keys_lock:
                            if known_row_keys:
                                start_row_prefix = random.choice(known_row_keys)[:10] # Prefix of a globally known key

                    if not start_row_prefix:
                        start_row_prefix = f"thr{random.randint(0, NUM_THREADS-1)}".encode('utf-8')


                    scan_count = 0
                    # print(f"Thread {thread_id} scanning with prefix {start_row_prefix.decode() if start_row_prefix else 'None'}")
                    for key, data in table.scan(row_prefix=start_row_prefix, limit=5):
                        scan_count += 1
                    ops_done["scan"] += 1 # Counts the scan operation itself
                    # print(f"Thread {thread_id} scan yielded {scan_count} rows")


                else: # Should not happen if probabilities sum to 1
                    pass

                if (i + 1) % (num_ops // 5 or 1) == 0:
                    print(f"Thread {thread_id}: Completed {i+1}/{num_ops} ops. Stats: {ops_done}")

            except Exception as e_inner:
                print(f"Thread {thread_id}: Inner Op Error - {e_inner}")
                ops_done["error"] += 1
                # Potentially re-establish connection on certain errors, but be careful
                # if isinstance(e_inner, (happybase.hbase.ttypes.TIOError, BrokenPipeError)):
                #     print(f"Thread {thread_id}: Reconnecting due to {type(e_inner)}")
                #     if connection: connection.close()
                #     connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=30000)
                #     table = connection.table(TABLE_NAME)


        print(f"Thread {thread_id}: Finished. Final Ops: {ops_done}")
        return ops_done

    except Exception as e_outer:
        print(f"Thread {thread_id}: Worker Error - {e_outer}")
        ops_done["error"] += (num_ops - sum(ops_done.values())) # Count remaining as errors
        return ops_done
    finally:
        if connection:
            connection.close()

# --- Main Stress Test Logic ---
if __name__ == "__main__":
    # --- Setup: Create table and pre-populate some data ---
    print("Setting up initial data for mixed workload...")
    initial_keys_for_workers = []
    try:
        # Use a single connection for setup
        setup_connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=60000)
        if TABLE_NAME not in setup_connection.tables():
            print(f"Creating table: {TABLE_NAME.decode()} with CF: {COLUMN_FAMILY.decode()}")
            setup_connection.create_table(TABLE_NAME, {COLUMN_FAMILY: dict()})
        else:
            print(f"Table {TABLE_NAME.decode()} already exists. Consider clearing it for a fresh test.")
            # For a true fresh start, you might want to disable and drop the table here.
            # setup_connection.disable_table(TABLE_NAME)
            # setup_connection.delete_table(TABLE_NAME)
            # setup_connection.create_table(TABLE_NAME, {COLUMN_FAMILY: dict()})


        # Pre-populate some data for reads/scans to be more effective initially
        table = setup_connection.table(TABLE_NAME)
        num_initial_rows = 50
        print(f"Pre-populating {num_initial_rows} rows...")
        for i in range(num_initial_rows):
            row_key = generate_row_key(prefix="initial")
            data_col = generate_random_data(DATA_SIZE_BYTES)
            table.put(row_key, {f'{COLUMN_FAMILY.decode()}:data'.encode('utf-8'): data_col})
            initial_keys_for_workers.append(row_key)
            if (i+1) % 10 == 0: print(f"Pre-populated {i+1}/{num_initial_rows}")
        
        with known_row_keys_lock: # Also add these to the global list
            known_row_keys.extend(initial_keys_for_workers)

        print(f"Pre-populated {len(initial_keys_for_workers)} initial rows.")
        setup_connection.close()
    except Exception as e:
        print(f"HBase setup error: {e}")
        if 'setup_connection' in locals() and setup_connection: setup_connection.close()
        exit()

    threads = []
    all_ops_summary = {"put": 0, "get_found": 0, "get_not_found": 0, "scan": 0, "error": 0}
    start_time = time.time()

    print(f"Starting Mixed workload stress test with {NUM_THREADS} threads, {OPERATIONS_PER_THREAD} ops/thread.")

    thread_results = []
    for i in range(NUM_THREADS):
        # Pass a copy of initial keys to each worker
        thread = threading.Thread(target=lambda: thread_results.append(mixed_worker(i, OPERATIONS_PER_THREAD, list(initial_keys_for_workers))))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()

    # Aggregate results
    for res in thread_results:
        if res: # Ensure worker returned a dict
            for key in all_ops_summary:
                all_ops_summary[key] += res.get(key, 0)


    total_time = end_time - start_time
    total_operations_attempted = NUM_THREADS * OPERATIONS_PER_THREAD
    total_operations_completed = sum(all_ops_summary.values()) - all_ops_summary["error"]


    print("\n--- Mixed Workload Stress Test Summary ---")
    print(f"Total operations attempted: {total_operations_attempted}")
    print(f"Total operations completed successfully: {total_operations_completed}")
    print(f"Detailed breakdown: {all_ops_summary}")
    print(f"Total time taken: {total_time:.2f} seconds")
    if total_time > 0 and total_operations_completed > 0:
        print(f"Average successful operations per second: {total_operations_completed / total_time:.2f}")
    else:
        print("Not enough successful operations or time to calculate ops/sec accurately.")