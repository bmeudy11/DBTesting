import pymongo
import threading
import time
import random
import string
from bson.objectid import ObjectId # For querying by _id

# --- Configuration ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "stress_test_db_mixed"
COLLECTION_NAME = "test_collection_mixed"
NUM_THREADS = 10
OPERATIONS_PER_THREAD = 5000
DOCUMENT_SIZE_KB = 5.5

# Operation mix (probabilities should sum to 1.0)
PROB_INSERT = 0.40
PROB_READ = 0.35
PROB_UPDATE = 0.15
PROB_DELETE = 0.10 # Deletes can make it harder to guarantee reads/updates find docs

# --- Helper Functions ---
def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def generate_document(size_kb):
    return {
        "payload": generate_random_string(int(size_kb * 1024)),
        "created_at": time.time(),
        "updated_at": time.time(),
        "version": 1,
        "processed": False
    }

# Store some ObjectIds of inserted documents for reads/updates/deletes
# This needs to be thread-safe if multiple threads modify it.
# For simplicity here, each thread will mostly operate on its own recent inserts,
# or we'll pre-populate some data. A shared list requires a Lock.
# Let's pre-populate for more reliable read/update/delete targets.

inserted_ids = [] # This will be populated initially

# --- Worker Function ---
def mixed_worker(thread_id, num_ops, client): # Pass client to reuse connection
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    ops_done = {"insert": 0, "read": 0, "update": 0, "delete": 0, "error": 0}
    local_inserted_ids = [] # IDs inserted by this thread

    print(f"Thread {thread_id}: Starting {num_ops} mixed operations.")

    for i in range(num_ops):
        op_choice = random.random()
        try:
            if op_choice < PROB_INSERT:
                # INSERT
                doc = generate_document(DOCUMENT_SIZE_KB)
                doc["thread_id"] = thread_id
                result = collection.insert_one(doc)
                local_inserted_ids.append(result.inserted_id)
                ops_done["insert"] += 1
            elif op_choice < PROB_INSERT + PROB_READ:
                # READ
                target_id = None
                if local_inserted_ids: # Prefer reading own inserts
                    target_id = random.choice(local_inserted_ids)
                elif inserted_ids: # Fallback to globally known IDs
                     target_id = random.choice(inserted_ids)

                if target_id:
                    doc = collection.find_one({"_id": target_id})
                    # if doc: print(f"Thread {thread_id}: Read doc {target_id}")
                else: # Or a broader query
                    # Example: find a random document, could be slow without good indexing
                    count = collection.count_documents({"processed": False})
                    if count > 0:
                        random_skip = random.randint(0, max(0, count -1))
                        doc = collection.find_one({"processed": False}, skip=random_skip)
                ops_done["read"] += 1
            elif op_choice < PROB_INSERT + PROB_READ + PROB_UPDATE:
                # UPDATE
                target_id = None
                if local_inserted_ids:
                    target_id = random.choice(local_inserted_ids)
                elif inserted_ids:
                     target_id = random.choice(inserted_ids)

                if target_id:
                    new_payload = generate_random_string(50) # smaller update
                    collection.update_one(
                        {"_id": target_id},
                        {"$set": {"payload": new_payload, "updated_at": time.time(), "processed": True}, "$inc": {"version": 1}}
                    )
                    ops_done["update"] += 1
                else: # If no ID, maybe skip or convert to an insert?
                    ops_done["update"] += 1 # Count as an attempt
            else:
                # DELETE
                target_id = None
                if local_inserted_ids:
                    target_id = local_inserted_ids.pop(random.randrange(len(local_inserted_ids))) # Pop to avoid re-deleting
                elif inserted_ids: # This is problematic if multiple threads try to delete from global
                     # For a more robust test, deletes should be carefully managed or target specific thread data
                    pass # Skipping delete from global for simplicity to avoid race conditions on 'inserted_ids'

                if target_id:
                    collection.delete_one({"_id": target_id})
                    ops_done["delete"] += 1
                else:
                    ops_done["delete"] += 1 # Count as an attempt

            if (i + 1) % (num_ops // 5) == 0:
                 print(f"Thread {thread_id}: Completed {i+1}/{num_ops} operations. Stats: {ops_done}")

        except Exception as e:
            print(f"Thread {thread_id}: Error - {e}")
            ops_done["error"] += 1
    print(f"Thread {thread_id}: Finished. Final Ops: {ops_done}")
    return ops_done


# --- Main Stress Test Logic ---
if __name__ == "__main__":
    # --- Setup: Connect and pre-populate some data ---
    print("Setting up initial data...")
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        if COLLECTION_NAME in db.list_collection_names():
            print(f"Dropping existing collection: {COLLECTION_NAME}")
            db[COLLECTION_NAME].drop()

        collection = db[COLLECTION_NAME]
        # Pre-populate some documents for reads/updates if needed
        num_initial_docs = 200
        for _ in range(num_initial_docs):
            result = collection.insert_one(generate_document(DOCUMENT_SIZE_KB))
            inserted_ids.append(result.inserted_id)
        print(f"Pre-populated {len(inserted_ids)} documents.")
        # Create indexes for performance
        collection.create_index([("processed", pymongo.ASCENDING)])
        collection.create_index([("thread_id", pymongo.ASCENDING)])
        print("Indexes created on 'processed' and 'thread_id'.")
    except Exception as e:
        print(f"Setup error: {e}")
        client.close()
        exit()

    threads = []
    all_ops_done = {"insert": 0, "read": 0, "update": 0, "delete": 0, "error": 0}
    start_time = time.time()

    print(f"Starting mixed workload stress test with {NUM_THREADS} threads, {OPERATIONS_PER_THREAD} ops/thread.")

    # Create one client per thread (or use a connection pool if library supports it well with threads)
    # PyMongo's MongoClient is thread-safe, so you can share it,
    # but for high concurrency, one client per thread can sometimes avoid internal contention.
    # For this example, we'll create a client per thread for simplicity.
    # More advanced: use a single client and ensure your app logic is thread-safe.

    worker_clients = [pymongo.MongoClient(MONGO_URI) for _ in range(NUM_THREADS)]

    for i in range(NUM_THREADS):
        # Pass the dedicated client to the worker
        thread = threading.Thread(target=mixed_worker, args=(i, OPERATIONS_PER_THREAD, worker_clients[i]))
        threads.append(thread)
        thread.start()

    results = [] # To store results from threads if your worker returns them
    for thread in threads:
        thread.join()
        # If mixed_worker returned something, you'd collect it here.
        # For this example, we rely on print statements from threads or
        # would need to modify mixed_worker to return its ops_done dictionary.

    end_time = time.time()

    # Close all worker clients
    for wc in worker_clients:
        wc.close()
    client.close() # Close the main setup client

    total_time = end_time - start_time
    total_operations = NUM_THREADS * OPERATIONS_PER_THREAD # Attempted operations

    print("\n--- Mixed Workload Stress Test Summary ---")
    # Note: To get accurate total ops_done, you'd need to collect results from each thread.
    # The printout below is based on attempted operations.
    print(f"Total operations attempted: {total_operations}")
    print(f"Total time taken: {total_time:.2f} seconds")
    if total_time > 0:
        print(f"Average operations per second: {total_operations / total_time:.2f}")
    else:
        print("Total time was too short to calculate ops/sec accurately.")

    # For a more detailed summary, you'd aggregate the 'ops_done' from each thread.