import pymongo
import threading
import time
import random
import string

# --- Configuration ---
MONGO_URI = "mongodb://localhost:27017/"  # Your local MongoDB URI
DB_NAME = "stress_test_db"
COLLECTION_NAME = "test_collection"
NUM_THREADS = 10  # Number of concurrent clients
DOCUMENTS_PER_THREAD = 1000 # Documents each client will insert
DOCUMENT_SIZE_KB = 1  # Approximate size of each document in KB

# --- Helper Function to Generate Random Data ---
def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def generate_document(size_kb):
    return {
        "index": None, # Will be set by the worker
        "payload": generate_random_string(size_kb * 1024), # Approximate size
        "timestamp": time.time(),
        "thread_id": None # Will be set by the worker
    }

# --- Worker Function for Each Thread ---
def insert_worker(thread_id, num_docs):
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Thread {thread_id}: Starting to insert {num_docs} documents.")
        for i in range(num_docs):
            doc = generate_document(DOCUMENT_SIZE_KB)
            doc["index"] = i
            doc["thread_id"] = thread_id
            collection.insert_one(doc)
            if (i + 1) % (num_docs // 10) == 0: # Log progress
                print(f"Thread {thread_id}: Inserted {i+1}/{num_docs} documents.")
        print(f"Thread {thread_id}: Finished inserting documents.")
    except Exception as e:
        print(f"Thread {thread_id}: Error - {e}")
    finally:
        if 'client' in locals():
            client.close()

# --- Main Stress Test Logic ---
if __name__ == "__main__":
    # --- Setup: Connect and potentially drop old collection ---
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        if COLLECTION_NAME in db.list_collection_names():
            print(f"Dropping existing collection: {COLLECTION_NAME}")
            db[COLLECTION_NAME].drop()
        print(f"Creating collection: {COLLECTION_NAME}")
        # You can also create indexes here if you want to test with them
        # db[COLLECTION_NAME].create_index([("thread_id", pymongo.ASCENDING)])
        client.close()
    except Exception as e:
        print(f"Setup error: {e}")
        exit()

    threads = []
    start_time = time.time()

    print(f"Starting stress test with {NUM_THREADS} threads, {DOCUMENTS_PER_THREAD} docs/thread.")

    for i in range(NUM_THREADS):
        thread = threading.Thread(target=insert_worker, args=(i, DOCUMENTS_PER_THREAD))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()
    total_time = end_time - start_time
    total_documents = NUM_THREADS * DOCUMENTS_PER_THREAD

    print("\n--- Stress Test Summary ---")
    print(f"Total documents inserted: {total_documents}")
    print(f"Total time taken: {total_time:.2f} seconds")
    if total_time > 0:
        print(f"Average inserts per second: {total_documents / total_time:.2f}")
    else:
        print("Total time was too short to calculate inserts per second accurately.")

    # --- Optional: Clean up ---
    # try:
    #     client = pymongo.MongoClient(MONGO_URI)
    #     db = client[DB_NAME]
    #     # db[COLLECTION_NAME].drop()
    #     # print(f"Cleaned up: Dropped collection {COLLECTION_NAME}")
    #     client.close()
    # except Exception as e:
    #     print(f"Cleanup error: {e}")