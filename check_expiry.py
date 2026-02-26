import os
import oracledb
import psycopg2
import argparse
import logging
import concurrent.futures
from pathlib import Path


logger = logging.getLogger(__name__)

def check_oracle_expiry(db_alias: str, sql_query: str):
    """
    Connects to a single Oracle database and checks user expiry status.
    """
    logger.info(f"[Oracle] Checking database: {db_alias}...")
    try:
        # Connect using external password store (wallet)
        with oracledb.connect(dsn=f"/@{db_alias}") as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                row = cursor.fetchone()
                
                if row:
                    expiry_days = row[0]
                    # Handle potential None values if the query returns NULL
                    if expiry_days is None:
                         logger.warning(f"{db_alias} [Oracle]: Could not determine expiry days (NULL returned).")
                         return

                    if expiry_days < 0:
                        logger.error(f"{db_alias} [Oracle]: User is EXPIRED! (Expiry days: {expiry_days})")
                    elif expiry_days < 5:
                        logger.warning(f"{db_alias} [Oracle]: User is expiring soon! (Expiry days: {expiry_days})")
                    else:
                        logger.info(f"{db_alias} [Oracle]: Account status nominal. (Expiry days: {expiry_days})")
                else:
                    logger.info(f"{db_alias} [Oracle]: No rows returned from query.")

    except oracledb.Error as e:
        logger.error(f"Failed to connect or query {db_alias} [Oracle]: {e}")
    except Exception as e:
         logger.error(f"{db_alias} [Oracle]: An unexpected error occurred: {e}")

def check_postgresql_expiry(db_alias: str, sql_query: str):
    """
    Connects to a single PostgreSQL database and checks user expiry status.
    Uses .pgpass for password authentication.
    """
    logger.info(f"[PostgreSQL] Checking database: {db_alias}...")
    try:
        # Connect using PostgreSQL connection string/dsn, will automatically use .pgpass file if matching
        with psycopg2.connect(db_alias) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                row = cursor.fetchone()
                
                if row:
                    expiry_days = row[0]
                    # Handle potential None values if the query returns NULL
                    if expiry_days is None:
                         logger.warning(f"{db_alias} [PostgreSQL]: Could not determine expiry days (NULL returned).")
                         return

                    if expiry_days < 0:
                        logger.error(f"{db_alias} [PostgreSQL]: User is EXPIRED! (Expiry days: {expiry_days})")
                    elif expiry_days < 5:
                        logger.warning(f"{db_alias} [PostgreSQL]: User is expiring soon! (Expiry days: {expiry_days})")
                    else:
                        logger.info(f"{db_alias} [PostgreSQL]: Account status nominal. (Expiry days: {expiry_days})")
                else:
                    logger.info(f"{db_alias} [PostgreSQL]: No rows returned from query.")

    except psycopg2.Error as e:
        logger.error(f"Failed to connect or query {db_alias} [PostgreSQL]: {e}")
    except Exception as e:
         logger.error(f"{db_alias} [PostgreSQL]: An unexpected error occurred: {e}")

def check_db_expiry(db_entry: str, sql_queries: dict):
    """
    Dispatches to the appropriate database engine checker based on the entry format (engine:alias).
    """
    if ':' not in db_entry:
        logger.error(f"Invalid format for '{db_entry}'. Expected engine:dbname (e.g., oracle:mydb)")
        return
        
    engine, db_alias = db_entry.split(':', 1)
    engine = engine.strip().lower()
    db_alias = db_alias.strip()
    
    sql_query = sql_queries.get(engine)
    if not sql_query:
        logger.error(f"No SQL query loaded for engine '{engine}'.")
        return

    if engine == 'oracle':
        check_oracle_expiry(db_alias, sql_query)
    elif engine == 'postgresql':
        check_postgresql_expiry(db_alias, sql_query)
    else:
        logger.error(f"Unsupported database engine '{engine}' for entry '{db_entry}'")

def get_db_list(file_path: Path) -> list[str]:
    """Reads the database list from a file, ignoring comments."""
    if not file_path.exists():
        logger.error(f"Database list file not found: {file_path}")
        return []
        
    try:
        with open(file_path, 'r') as f:
            return [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
    except Exception as e:
        logger.error(f"Error reading database list file: {e}")
        return []

def load_sql_query(file_path: Path) -> str:
    """Reads the SQL query from a file."""
    if not file_path.exists():
        logger.error(f"SQL file not found: {file_path}")
        return ""

    try:
        with open(file_path, 'r') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading SQL file: {e}")
        return ""

def main():
    parser = argparse.ArgumentParser(description="Check database password expiry.")
    parser.add_argument("--dblist", type=Path, default=Path(__file__).parent / "config/dblist.lst",
                        help="Path to the file containing the list of databases.")
    parser.add_argument("--workers", type=int, default=5,
                        help="Number of parallel workers.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level.")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Initialize Oracle Client
    try:
        oracledb.init_oracle_client()
    except Exception as e:
         logger.error(f"Failed to initialize Oracle Client: {e}")
         return

    databases = get_db_list(args.dblist)
    if not databases:
        logger.error("No databases to check.")
        return

    engines_to_check = set()
    for db in databases:
        if ':' in db:
            engines_to_check.add(db.split(':', 1)[0].strip().lower())

    sql_dir = Path(__file__).parent / "sql"
    sql_queries = {}
    
    if 'oracle' in engines_to_check:
        sql_queries['oracle'] = load_sql_query(sql_dir / "orcl_expire_check.sql")
    if 'postgresql' in engines_to_check:
        sql_queries['postgresql'] = load_sql_query(sql_dir / "pg_expire_check.sql")

    if not any(sql_queries.values()):
        logger.error("No valid SQL queries could be loaded.")
        return

    logger.info(f"Starting expiry check for {len(databases)} databases with {args.workers} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        futures = [executor.submit(check_db_expiry, db, sql_queries) for db in databases]
        
        # Wait for all tasks to complete
        concurrent.futures.wait(futures)

    logger.info("Expiry check completed.")

if __name__ == "__main__":
    main()
