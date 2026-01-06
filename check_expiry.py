import os
import oracledb
import argparse
import logging
import concurrent.futures
from pathlib import Path


logger = logging.getLogger(__name__)

def check_db_expiry(db_alias: str, sql_query: str):
    """
    Connects to a single database and checks user expiry status.
    """
    logger.info(f"Checking database: {db_alias}...")
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
                         logger.warning(f"{db_alias}: Could not determine expiry days (NULL returned).")
                         return

                    if expiry_days < 0:
                        logger.error(f"{db_alias}: User is EXPIRED! (Expiry days: {expiry_days})")
                    elif expiry_days < 5:
                        logger.warning(f"{db_alias}: User is expiring soon! (Expiry days: {expiry_days})")
                    else:
                        logger.info(f"{db_alias}: Account status nominal. (Expiry days: {expiry_days})")
                else:
                    logger.info(f"{db_alias}: No rows returned from query.")

    except oracledb.Error as e:
        logger.error(f"Failed to connect or query {db_alias}: {e}")
    except Exception as e:
         logger.error(f"{db_alias}: An unexpected error occurred: {e}")

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
    parser = argparse.ArgumentParser(description="Check Oracle database password expiry.")
    parser.add_argument("--dblist", type=Path, default=Path(__file__).parent / "config/dblist.lst",
                        help="Path to the file containing the list of databases.")
    parser.add_argument("--sql", type=Path, default=Path(__file__).parent / "sql/expire_check.sql",
                        help="Path to the SQL file.")
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

    sql_query = load_sql_query(args.sql)
    if not sql_query:
        logger.error("SQL query is empty or could not be loaded.")
        return

    logger.info(f"Starting expiry check for {len(databases)} databases with {args.workers} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        futures = [executor.submit(check_db_expiry, db, sql_query) for db in databases]
        
        # Wait for all tasks to complete
        concurrent.futures.wait(futures)

    logger.info("Expiry check completed.")

if __name__ == "__main__":
    main()
