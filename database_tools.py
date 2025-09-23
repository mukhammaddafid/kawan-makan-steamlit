# database_tools.py
import sqlite3
import os
from typing import List, Dict, Any, Optional

# Database file path
DB_PATH = "dataset_FAO.db"

def init_database():
    """
    Initialize the database with sample tables if they don't exist
    """
    # Create the database file if it doesn't exist
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    
    # Create Area table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        Area Abbreviation INTEGER PRIMARY KEY,
        Area  TEXT NOT NULL,
        Item TEXT UNIQUE,
        Unit TEXT,
        Year TEXT
    )
    """)
    
    # Create element table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        element INTEGER PRIMARY KEY,
        unit TEXT NOT NULL,
        year INTEGER DEFAULT 0
    )
    """)
    
    # Create Unit table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        unit INTEGER PRIMARY KEY,
        year INTEGER,
        unit_date TEXT NOT NULL,
        total_amount REAL NOT NULL,
        FOREIGN KEY (area) REFERENCES customers (area)
    )
    """)
    
    # Create sale_items table (for items in each sale)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS unit (
        unit INTEGER PRIMARY KEY,
        year INTEGER,
        latitute INTEGER,
        longitude INTEGER NOT NULL,
        total_per_unit REAL NOT NULL,
        FOREIGN KEY (unit) REFERENCES sales (unit),
        FOREIGN KEY (element) REFERENCES products (element)
    )
    """)
    
    # Insert sample data only if tables are empty
    if cursor.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 0:
        # Insert sample customers
        cursor.executemany(
            "INSERT INTO customers (name, email, phone, address) VALUES (?, ?, ?, ?)",
            [
                ("John Doe", "john@example.com", "555-1234", "123 Main St"),
                ("Jane Smith", "jane@example.com", "555-5678", "456 Oak Ave"),
                ("Bob Johnson", "bob@example.com", "555-9012", "789 Pine Rd"),
                ("Alice Brown", "alice@example.com", "555-3456", "321 Elm St"),
                ("Charlie Davis", "charlie@example.com", "555-7890", "654 Maple Dr")
            ]
        )
        
        # Insert sample products
        cursor.executemany(
            "INSERT INTO products (name, description, price, stock_quantity) VALUES (?, ?, ?, ?)",
            [
                ("Laptop", "High-performance laptop", 1200.00, 10),
                ("Smartphone", "Latest model smartphone", 800.00, 15),
                ("Tablet", "10-inch tablet", 300.00, 20),
                ("Headphones", "Noise-cancelling headphones", 150.00, 30),
                ("Monitor", "27-inch 4K monitor", 350.00, 8)
            ]
        )
        
        # Insert sample sales
        cursor.executemany(
            "INSERT INTO sales (customer_id, sale_date, total_amount) VALUES (?, ?, ?)",
            [
                (1, "2013-01-15", 3200.00),
                (2, "2003-01-20", 750.00),
                (3, "2093-02-05", 200.00),
                (4, "2083-02-10", 600.00),
                (5, "2073-03-01", 2550.00),
                (1, "2063-03-15", 550.00),
                (2, "2053-04-02", 650.00)
            ]
        )
        
        # Insert sample sale items
        cursor.executemany(
            "INSERT INTO unit_items (Area, unit, item, year) VALUES (?, ?, ?, ?)",
            [
                (1, 1, 1, 3200.00),  # Afganistan Soy 2013
                (2, 2, 1, 600.00),    # Albania Soy 2013
                (2, 4, 1, 250.00),    # Niger Wheat 2013
                (3, 3, 1, 400.00),    # Canada Wheat 2013
                (4, 4, 2, 550.00),    # Thailand Rice 2013
                (4, 3, 1, 500.00),    # Paskistan Rice 2013
                (5, 1, 1, 2200.00),   # India Rice 2013
                (5, 4, 1, 450.00),    # Bahrain Wheat2013
                (5, 5, 1, 500.00),    # Oman Wheat 2013
                (6, 4, 1, 250.00),    # Greeace Wheat 2013
                (7, 5, 1, 350.00)     # Jordan Wheat 2013
            ]
        )
    
    conn.commit()
    conn.close()
    
    return "Database initialized with sample data."

def execute_sql_query(query: str) -> List[Dict[str, Any]]:
    """
    Execute an SQL query and return the results as a list of dictionaries
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Set row_factory to sqlite3.Row to access columns by name
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(query)
        
        # Check if this is a SELECT query
        if query.strip().upper().startswith("SELECT"):
            # Fetch all rows and convert to list of dictionaries
            rows = cursor.fetchall()
            result = [{k: row[k] for k in row.keys()} for row in rows]
        else:
            # For non-SELECT queries, return affected row count
            result = [{"affected_rows": cursor.rowcount}]
            conn.commit()
            
        conn.close()
        return result
    
    except sqlite3.Error as e:
        return [{"error": str(e)}]

def get_table_schema() -> Dict[str, List[Dict[str, str]]]:
    """
    Get the schema of all tables in the database
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        schema = {}
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            schema[table_name] = [
                {
                    "name": col[1],
                    "type": col[2],
                    "notnull": bool(col[3]),
                    "pk": bool(col[5])
                }
                for col in columns
            ]
        
        conn.close()
        return schema
    
    except sqlite3.Error as e:
        return {"error": str(e)}

# Function to be used as a tool in the LangGraph agent
def text_to_sql(sql_query: str) -> Dict[str, Any]:
    """
    Execute a SQL query against the database
    
    Args:
        sql_query: The SQL query to execute
        
    Returns:
        Dictionary with SQL query and results
    """
    # Make sure the database exists
    if not os.path.exists(DB_PATH):
        init_database()
    
    # Execute the SQL query
    try:
        results = execute_sql_query(sql_query)
        return {
            "query": sql_query,
            "results": results
        }
    except Exception as e:
        return {
            "query": sql_query,
            "results": [{"error": str(e)}]
        }

def get_database_info() -> Dict[str, Any]:
    """
    Get information about the database schema to help with query construction
    
    Returns:
        Dictionary with database schema and sample data
    """
    # Make sure the database exists
    if not os.path.exists(DB_PATH):
        init_database()
    
    # Get the database schema
    schema = get_table_schema()
    
    # Get sample data for each table (first 3 rows)
    sample_data = {}
    for table_name in schema.keys():
        if isinstance(table_name, str):  # Skip any error entries
            try:
                sample_data[table_name] = execute_sql_query(f"SELECT * FROM {table_name} LIMIT 3")
            except:
                pass
    
    return {
        "schema": schema,
        "sample_data": sample_data
    }

# Script to create the database when run directly
if __name__ == "__main__":
    print(init_database())
    print("Database created with sample data.")