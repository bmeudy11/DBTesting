import happybase

    # Replace with your HBase host and port if needed
connection = happybase.Connection('localhost', port=9090)

try:
    # Interact with HBase
    print("Connection successful!")
    # List tables
    print("Tables:", connection.tables())

    # Example: Access a table (replace 'my_table' with your table name)
    table = connection.table('my_table')
    print("Accessed table:", table.name)

except Exception as e:
    print(f"Error: {e}")

finally:
    connection.close()