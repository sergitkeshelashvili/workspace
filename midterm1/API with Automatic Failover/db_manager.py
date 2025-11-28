import psycopg2
from psycopg2 import OperationalError
import contextlib
import logging

# --- Configuration ---
DB_USER = 'postgres'
DB_PASS = '55555' # <--
DB_NAME = 'analytics_db'

# NOTE: Both Master and Slave share the same IP on WSL instance
DB_HOST = '172.27.142.53' 

MASTER_PORT = 5532
SLAVE_PORT = 5434

MASTER_CONN_PARAMS = {
    'host': DB_HOST,
    'port': MASTER_PORT,
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASS,
}

SLAVE_CONN_PARAMS = {
    'host': DB_HOST,
    'port': SLAVE_PORT,
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASS,
}
# ----------------------------------------------------------------

logger = logging.getLogger('db_manager')

@contextlib.contextmanager
def FailoverConnectionManager():
    """
    Context manager for handling connection and failover logic.
    Yields a tuple: (connection_object, server_name).
    """
    target_server = "Slave"
    conn = None
    
    try:  # OUTER TRY BLOCK (Attempt Slave connection)
        
        # 1. Attempt connection to the SLAVE
        conn_string = (
            f"host='{SLAVE_CONN_PARAMS['host']}' port='{SLAVE_CONN_PARAMS['port']}' "
            f"dbname='{SLAVE_CONN_PARAMS['dbname']}' user='{SLAVE_CONN_PARAMS['user']}' "
            f"password='{SLAVE_CONN_PARAMS['password']}'"
        )
        conn = psycopg2.connect(conn_string) 
        
        # SUCCESS PATH (Slave): Yield the connection and the server name
        logger.info(f"Connection established with {target_server}.")
        yield conn, target_server 
        return

    except OperationalError: # OUTER EXCEPT BLOCK (Slave failed)
        
        # 2. SLAVE failed, attempt failover to the MASTER
        logger.warning("Slave is down/unavailable. Attempting failover to Master...")
        target_server = "Master"
        
        try: # INNER TRY BLOCK (Master connection attempt)
            conn_string = (
                f"host='{MASTER_CONN_PARAMS['host']}' port='{MASTER_CONN_PARAMS['port']}' "
                f"dbname='{MASTER_CONN_PARAMS['dbname']}' user='{MASTER_CONN_PARAMS['user']}' "
                f"password='{MASTER_CONN_PARAMS['password']}'"
            )
            conn = psycopg2.connect(conn_string) 
            
            # SUCCESS PATH (Master): Yield the connection and the server name
            logger.info("Successfully failed over to Master.")
            yield conn, target_server
            return

        except OperationalError as e_master: # INNER EXCEPT BLOCK (Master failed)
            
            # 3. Both servers failed
            logger.error("Failover failed: Both Slave and Master are unavailable.")
            raise OperationalError(f"Database cluster unavailable: {e_master}")

    finally: # FINAL BLOCK (Runs regardless of success/failure)
        # 4. Ensure connection is closed
        if conn:
            conn.close()

def get_db_results(query: str, query_params: tuple = None):
    """
    Executes a read query using the failover logic and returns results and the server source.
    """
    # NOTE: We unpack the tuple (conn, server) here!
    with FailoverConnectionManager() as (conn, server): 
        # The connection will be either to Master or Slave, or it will raise an error
        with conn.cursor() as cur:
            cur.execute(query, query_params)

            # Fetch results and format into a list of dictionaries
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
                return results, server # Return the server name from the unpacked tuple
            return None, server

