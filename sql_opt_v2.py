import mysql.connector
import time
import re
import schedule

# ------------------------------
# Part 1: SQL Execution Plan Analysis & Index Suggestion
# ------------------------------

def get_execution_plan(conn, query):
    """
    Executes an EXPLAIN query to retrieve the execution plan for a given SQL statement.
    
    :param conn: A MySQL database connection object.
    :param query: The SQL query string for which the execution plan is requested.
    :return: The execution plan as a list of tuples (result of EXPLAIN query).
    """
    cursor = conn.cursor(buffered=True)
    # Prepend the EXPLAIN statement to the original query
    explain_query = "EXPLAIN " + query
    cursor.execute(explain_query)
    plan = cursor.fetchall()
    cursor.close()
    return plan

def run_query(conn, query):
    """
    Executes the given query and returns the elapsed time in seconds.
    For SELECT queries, this function fetches all results to clear the result set.
    
    :param conn: A MySQL database connection object.
    :param query: The SQL query string to be executed.
    :return: Elapsed time in seconds for running the query.
    """
    cursor = conn.cursor(buffered=True)
    start = time.time()
    cursor.execute(query)
    
    # If it's a SELECT query, fetch results to ensure the entire result set is processed
    if query.strip().upper().startswith("SELECT"):
        cursor.fetchall()
    else:
        # For non-SELECT queries, commit any changes (INSERT, UPDATE, DELETE, etc.)
        conn.commit()
    
    elapsed = time.time() - start
    cursor.close()
    return elapsed

def extract_columns_from_where(query):
    """
    Extracts column names from the WHERE clause using a basic regex approach.
    Assumes conditions appear in the form: column operator value.
    
    :param query: The SQL query from which to extract column names in the WHERE clause.
    :return: A list of column names used in the WHERE clause.
    """
    # Attempt to locate the WHERE clause
    where_clause = re.search(r'\bWHERE\b(.*)', query, re.IGNORECASE)
    if not where_clause:
        return []
    
    conditions = where_clause.group(1)
    # Split by AND/OR (basic approach)
    parts = re.split(r'\bAND\b|\bOR\b', conditions, flags=re.IGNORECASE)
    
    columns = []
    for part in parts:
        # Look for the pattern: column operator ...
        match = re.match(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|>|<|>=|<=)', part.strip())
        if match:
            columns.append(match.group(1))
    
    return columns

def analyze_plan_and_suggest(plan, query):
    """
    Analyzes the EXPLAIN plan output and provides index suggestions if a full table scan is detected.
    
    :param plan: The execution plan returned by the EXPLAIN query (list of tuples).
    :param query: Original SQL query used in EXPLAIN.
    :return: A dictionary of {table_name: suggestion_message}.
    """
    suggestions = {}
    # Identify columns used in the WHERE clause
    candidate_columns = extract_columns_from_where(query)
    
    for row in plan:
        """
        Typical MySQL EXPLAIN columns:
         - id
         - select_type
         - table
         - type
         - possible_keys
         - key
         - key_len
         - ref
         - rows
         - Extra
        """
        id_val, select_type, table, type_val, possible_keys, key, key_len, ref, rows, extra = row
        
        # Check if the query is doing a full table scan (type = "ALL")
        if type_val.upper() == "ALL":
            # If no index is used or possible_keys is None/empty, consider recommending an index
            if possible_keys is None or key is None:
                if candidate_columns:
                    suggestion = (
                        f"Consider adding an index on table '{table}' "
                        f"for columns: {', '.join(candidate_columns)}."
                    )
                else:
                    # If we don't have identified columns, provide a generic suggestion
                    suggestion = (
                        f"Review the WHERE clause for table '{table}' "
                        f"and consider adding appropriate indexes."
                    )
                suggestions[table] = suggestion
    
    return suggestions

def sql_analysis_demo(conn):
    """
    Demonstrates how to:
     1) Get the execution plan of a query
     2) Analyze the plan for potential indexing improvements
     3) Compare execution times of an original vs. an "optimized" query
    
    :param conn: A MySQL database connection object.
    """
    # Define an example query for analysis (Replace placeholders with actual table & columns)
    original_query = "SELECT [column_name] FROM [TABLE] FORCE INDEX (PRIMARY) WHERE [WHERE_OPTIONS];"
    
    print("=== Original Query Execution Plan ===")
    plan = get_execution_plan(conn, original_query)
    for row in plan:
        print(row)
    
    # Analyze the plan and gather index improvement suggestions
    suggestions = analyze_plan_and_suggest(plan, original_query)
    if suggestions:
        print("\nIndex Improvement Suggestions:")
        for table, suggestion in suggestions.items():
            print(f"- {suggestion}")
    else:
        print("\nNo index suggestions based on the current execution plan.")
    
    # Run the original query and record time
    original_time = run_query(conn, original_query)
    print(f"\nOriginal execution time: {original_time:.4f} seconds")
    
    # Define an "optimized" query (replace with actual improved query)
    optimized_query = "SELECT [column_name] FROM [TABLE] FORCE INDEX (PRIMARY) WHERE [WHERE_OPTIONS];"
    
    print("\n=== Optimized Query Execution Plan ===")
    optimized_plan = get_execution_plan(conn, optimized_query)
    for row in optimized_plan:
        print(row)
    
    # Run the optimized query and record time
    optimized_time = run_query(conn, optimized_query)
    print(f"\nOptimized execution time: {optimized_time:.4f} seconds")
    
    # Calculate performance improvement in percentage
    if original_time:
        improvement = ((original_time - optimized_time) / original_time) * 100
    else:
        improvement = 0
    print(f"\nPerformance improvement: {improvement:.2f}%\n")

# ------------------------------
# Part 2: Automating Repetitive Tasks & Transaction Handling
# ------------------------------

def automated_task(conn):
    """
    Demonstrates an automated task that:
     - Shows usage of transactions for data integrity.
     - Performs routine queries (counting rows, checking status variables).
    
    :param conn: A MySQL database connection object.
    """
    try:
        cursor = conn.cursor(buffered=True)
        print("Starting automated task...")
        
        # Check if a transaction is already active; if not, start a new one
        if not conn.in_transaction:
            conn.start_transaction()
        
        # Example repetitive work: counting rows in a table (e.g., 'users')
        cursor.execute("SELECT COUNT(*) FROM users;")
        count = cursor.fetchone()[0]
        print(f"User count: {count}")
        
        # Check a specific memory/status variable from MySQL server
        cursor.execute("SHOW STATUS LIKE 'Innodb_buffer_pool_bytes_data';")
        buffer_status = cursor.fetchone()
        if buffer_status:
            print(f"InnoDB Buffer Pool Data: {buffer_status[1]}")
        
        # Commit the transaction after successful operations
        conn.commit()
        print("Automated task completed successfully.\n")
    
    except mysql.connector.Error as err:
        # If there's an error during the automated task, rollback changes
        print(f"Error during automated task: {err}")
        conn.rollback()
    finally:
        cursor.close()

def automation_demo(conn):
    """
    Demonstrates how to schedule repetitive tasks using the 'schedule' library.
    
    :param conn: A MySQL database connection object.
    """
    # Schedule the automated_task to run every minute
    schedule.every(1).minutes.do(automated_task, conn)
    
    print("Starting scheduled automated tasks. Press Ctrl+C to exit.")
    try:
        # Keep the script running to allow scheduled tasks to execute
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting scheduled tasks.")

# ------------------------------
# Main Execution
# ------------------------------

def main():
    """
    Main function to:
     1) Connect to the MySQL database
     2) Demonstrate SQL analysis and query optimization
     3) Demonstrate automation of repetitive tasks
    """
    # Update connection details with valid credentials
    conn = mysql.connector.connect(
        host="",
        user="",
        password="",
        database=""
    )
    
    # Demonstrate SQL execution plan analysis and optimization
    sql_analysis_demo(conn)
    
    # Demonstrate automation of repetitive DB tasks with transaction management
    automation_demo(conn)
    
    # Close the connection when done
    conn.close()

if __name__ == "__main__":
    main()
