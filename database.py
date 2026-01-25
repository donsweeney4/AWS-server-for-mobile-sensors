import logging
import mysql.connector
from mysql.connector import Error
from config import Config

logger = logging.getLogger(__name__)

#####################################################################
def get_db_connection():
    try:
        logger.info("🔵 Attempting database connection with the following parameters:")
        for key, value in Config.DB_CONFIG.items():
            if key == 'password':
                logger.info(f"{key}: ********")
            else:
                logger.info(f"{key}: {value}")

        conn = mysql.connector.connect(**Config.DB_CONFIG)

        if conn.is_connected():
            logger.info("🟢 Successfully connected to the database")
            return conn
        else:
            logger.error("❌ Connection established but is_connected() returned False")
            raise Error("Connection failed")
    except Error as e:
        logger.error(f"❌ Error connecting to database: {e}")
        raise

#####################################################################
def fetch_all_rows(query, args=None):

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, args or ())
        rows = cursor.fetchall()
        logger.info("Query executed successfully")
        return rows
    except Error as e:
        logger.error(f"❌ Error executing query: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

#####################################################################
def execute_db_update(query, params):
    """   
    Executes a parameterized UPDATE/INSERT/DELETE query 
    """
    logger.info("Executing DB Update:")
    logger.info(f"  Query: {query}")
    logger.info(f"  Params: {params}")

    conn = None # Initialize conn to None
    cursor = None # Initialize cursor to None
    try:
        conn = get_db_connection() # Get connection
        cursor = conn.cursor() # Get cursor
        cursor.execute(query, params) # Execute query
        conn.commit() # Commit changes *only if* execute succeeded
        logger.info("DB Update committed successfully.")
        return True # Explicitly return True on success

    except Exception as e:
        logger.error(f"Database Update Error: {e}", exc_info=True) # Log full traceback
        if conn:
            try:
                conn.rollback() # Rollback on error
                logger.info("DB Update rolled back due to error.")
            except Exception as rb_err:
                # Log rollback error but prioritize original exception
                logger.error(f"Error during rollback: {rb_err}", exc_info=True)
        raise # Re-raise the original exception to be caught by the route handler

    finally:
        # Ensure cursor and connection are closed even if errors occurred
        if cursor:
            try:
                cursor.close()
            except Exception as c_err:
                 logger.error(f"Error closing cursor: {c_err}", exc_info=True)
        if conn:
             try:
                # For pooled connections, close() usually returns it to the pool
                conn.close()
             except Exception as conn_err:
                  logger.error(f"Error closing connection: {conn_err}", exc_info=True)

#####################################################################

# Example usage
if __name__ == "__main__":
    try:
  
        query = "SELECT  campaign_id FROM mobile_metadata order by campaign_id"
   

        results = fetch_all_rows(query)
        for row in results:
            print(row)
    except Exception as e:
        print(f"An error occurred: {e}")
