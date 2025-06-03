import pymongo
import happybase
import threading
import time
import random
import string
import uuid
from tabulate import tabulate

#local mongo instance
MONGO_URI = 'mongodb://localhost:27017/'
#mongo db name
DB_NAME = 'stress_test_db'
#mongo collection name
COLLECTION_NAME = 'test_collection'
#concurrent threads, for stress testing
NUM_THREADS = 10
#documents/records to insert per thread (ie 10thread x 1000documents = 10000 total docs)
DOCUMENTS_PER_THREAD = 1000
#size in kb of docs
DOCUMENT_SIZE_BYTES = 1024

#local hbase install, port
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
#hbase table name as bytes
TABLE_NAME = b'stress_test_table' # Table name as bytes
#column, column qualifier
COLUMN_FAMILY = b'cf1'
COLUMN_QUALIFIER = b'data'
# Rows each client will insert
ROWS_PER_THREAD = 1000
ROW_KEY_PREFIX = 'user_'
   

#generate random data for document
def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def generate_random_value(size_bytes):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(size_bytes)).encode('utf-8')

#function to generate document for mongo insert
def generate_document(sizeBytes):
    return {
        'index': None,
        'payload': generate_random_string(sizeBytes),
        'timestamp': time.time(),
        'thread_id': None
    }

#worker process to insert data into mongoDB with multiple threads
def mongoWorker(threadId, numDocs):
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print(f'Thread {threadId}: Starting to insert {numDocs} documents.')

        for i in range(numDocs):
            doc = generate_document(DOCUMENT_SIZE_BYTES)
            doc['index'] = i
            doc['thread_id'] = threadId
            collection.insert_one(doc)
            if (i + 1) % (numDocs // 10) == 0:
                print(f'Thread {threadId}: Inserted {i+1}/{numDocs} documents.')
        print(f'Thread {threadId}: Finished inserting documents.')
    except Exception as e:
        print(f'Thread {threadId}: Error - {e}')
    finally:
        #cleanup - close connection
        if 'client' in locals():
            client.close()

#worker process to insert data into HBase with multiple threads
def hbaseWorker(threadId, numRows, tableNameBytes):
    try:
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=30000) # 30s timeout
        table = connection.table(tableNameBytes)
        print(f'Thread {threadId}: Starting to insert {numRows} rows.')

        for i in range(numRows):
            row_key = f'{ROW_KEY_PREFIX}{threadId}_{i}_{uuid.uuid4()}'.encode('utf-8')

            value = generate_random_value(DOCUMENT_SIZE_BYTES)

            data_to_put = {
                f'{COLUMN_FAMILY.decode("utf-8")}:{COLUMN_QUALIFIER.decode("utf-8")}': value
            }

            table.put(row_key, data_to_put)

            if (i + 1) % (numRows // 10) == 0: # Log progress
                print(f'Thread {threadId}: Put {i+1}/{numRows} rows.')
    except Exception as e:
        print(f'Thread {threadId}: Error - {e}')
    finally:
        #cleanup - close connection
        if connection:
            connection.close()

if __name__ == '__main__':
    try:
        #*******************************************************************************************
        #MONGO CONNECTION
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]

        #MONGO - drop collection if it already exists
        if COLLECTION_NAME in db.list_collection_names():
            print(f'Collection {COLLECTION_NAME} already exists. Dropping collection for fresh start: {COLLECTION_NAME}')
            db[COLLECTION_NAME].drop()
        
        client.close()

        mongoThreads = []
        mongoStartTime = time.time()

        for i in range(NUM_THREADS):
            mongoThread = threading.Thread(target=mongoWorker, args=(NUM_THREADS, DOCUMENTS_PER_THREAD))
            mongoThreads.append(mongoThread)
            mongoThread.start()

        for thread in mongoThreads:
            thread.join()

        mongoEndTime = time.time()
        mongoTotalTime = mongoEndTime - mongoStartTime
        #*******************************************************************************************
        
        #*******************************************************************************************
        #HBASE CONNECTION 30s timeout
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=30000)
        table = connection.table(TABLE_NAME)

        #HBASE - clean up tables if already exist
        existing_tables = connection.tables()
        if TABLE_NAME in existing_tables:
            print(f"Table '{TABLE_NAME.decode('utf-8')}' already exists. Disabling and deleting for a fresh start.")
            connection.disable_table(TABLE_NAME)
            connection.delete_table(TABLE_NAME)
            print(f"Table '{TABLE_NAME.decode('utf-8')}' deleted.")

        families = {
            COLUMN_FAMILY.decode('utf-8'): dict() # Empty dict for default CF options
        }
        connection.create_table(TABLE_NAME, families)

        hbaseThreads = []
        hbaseStartTime = time.time()
        
        for i in range(NUM_THREADS):
            hbaseThread = threading.Thread(target=hbaseWorker, args=(i, ROWS_PER_THREAD, TABLE_NAME))
            hbaseThreads.append(hbaseThread)
            hbaseThread.start()

        for thread in hbaseThreads:
            thread.join()


        hbaseEndTime = time.time()
        hbaseTotalTime = hbaseEndTime - hbaseStartTime
        #*******************************************************************************************

        #Print Summaries
        data = [
            ['Total threads', NUM_THREADS, NUM_THREADS],
            ['Total inserts', DOCUMENTS_PER_THREAD, DOCUMENTS_PER_THREAD],
            ['Total time taken', f'{mongoTotalTime:.2f} s', f'{hbaseTotalTime:.2f} s']
        ]

        # Define the headers
        headers = ['Metric', 'mongoDB Summary', 'HBase Summary']

        # Print the table
        print(tabulate(data, headers=headers, tablefmt='grid'))

    except Exception as e:
        print(f'Error: {e}')
    finally:
        if connection:
            connection.close()