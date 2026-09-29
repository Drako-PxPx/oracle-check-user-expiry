import os
import sys
import oracledb
import psycopg2
import argparse
import logging
import concurrent.futures
from pathlib import Path


logger = logging.getLogger(__name__)

# Status values returned by the checkers, ordered by severity
STATUS_OK = "OK"
STATUS_EXPIRING = "EXPIRING"
STATUS_EXPIRED = "EXPIRED"
STATUS_LOCKED = "LOCKED"
STATUS_ERROR = "ERROR"

ORA_ACCOUNT_LOCKED = 28000
ORA_PASSWORD_EXPIRED = 28001


def evaluate_expiry(db_alias: str, engine_label: str, row, threshold: int) -> str:
    """Logs and returns the status for the expiry_days value returned by the query."""
    if not row:
        logger.info(f"{db_alias} [{engine_label}]: No rows returned from query.")
        return STATUS_OK

    expiry_days = row[0]
    if expiry_days is None:
        logger.info(f"{db_alias} [{engine_label}]: Password has no expiry date.")
        return STATUS_OK

    if expiry_days < 0:
        logger.error(f"{db_alias} [{engine_label}]: User is EXPIRED! (Expiry days: {expiry_days:.1f})")
        return STATUS_EXPIRED
    if expiry_days < threshold:
        logger.warning(f"{db_alias} [{engine_label}]: User is expiring soon! (Expiry days: {expiry_days:.1f})")
        return STATUS_EXPIRING

    logger.info(f"{db_alias} [{engine_label}]: Account status nominal. (Expiry days: {expiry_days:.1f})")
    return STATUS_OK

def check_oracle_expiry(db_alias: str, sql_query: str, threshold: int) -> str:
    """
    Connects to a single Oracle database and checks user expiry status.
    """
    logger.info(f"[Oracle] Checking database: {db_alias}...")
    try:
        # Connect using external password store (wallet)
        with oracledb.connect(dsn=f"/@{db_alias}") as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                return evaluate_expiry(db_alias, "Oracle", cursor.fetchone(), threshold)

    except oracledb.Error as e:
        # A fully expired or locked account can't log in, so the status
        # arrives as a connection error instead of a query result
        error_code = getattr(e.args[0], "code", None) if e.args else None
        if error_code == ORA_PASSWORD_EXPIRED:
            logger.error(f"{db_alias} [Oracle]: User is EXPIRED! (ORA-28001 at login)")
            return STATUS_EXPIRED
        if error_code == ORA_ACCOUNT_LOCKED:
            logger.error(f"{db_alias} [Oracle]: User is LOCKED! (ORA-28000 at login)")
            return STATUS_LOCKED
        logger.error(f"Failed to connect or query {db_alias} [Oracle]: {e}")
        return STATUS_ERROR
    except Exception as e:
         logger.error(f"{db_alias} [Oracle]: An unexpected error occurred: {e}")
         return STATUS_ERROR

def check_postgresql_expiry(db_alias: str, sql_query: str, threshold: int) -> str:
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
                return evaluate_expiry(db_alias, "PostgreSQL", cursor.fetchone(), threshold)

    except psycopg2.Error as e:
        # PostgreSQL reports an expired password as a plain authentication failure
        logger.error(f"Failed to connect or query {db_alias} [PostgreSQL] (password wrong or expired?): {e}")
        return STATUS_ERROR
    except Exception as e:
         logger.error(f"{db_alias} [PostgreSQL]: An unexpected error occurred: {e}")
         return STATUS_ERROR

def check_db_expiry(db_entry: str, sql_queries: dict, threshold: int) -> str:
    """
    Dispatches to the appropriate database engine checker based on the entry format (engine:alias).
    """
    if ':' not in db_entry:
        logger.error(f"Invalid format for '{db_entry}'. Expected engine:dbname (e.g., oracle:mydb)")
        return STATUS_ERROR
        
    engine, db_alias = db_entry.split(':', 1)
    engine = engine.strip().lower()
    db_alias = db_alias.strip()
    
    sql_query = sql_queries.get(engine)
    if not sql_query:
        logger.error(f"No SQL query loaded for engine '{engine}'.")
        return STATUS_ERROR

    if engine == 'oracle':
        return check_oracle_expiry(db_alias, sql_query, threshold)
    elif engine == 'postgresql':
        return check_postgresql_expiry(db_alias, sql_query, threshold)
    else:
        logger.error(f"Unsupported database engine '{engine}' for entry '{db_entry}'")
        return STATUS_ERROR

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

def main() -> int:
    """
    Returns the process exit code: 0 all nominal, 1 a password is expiring,
    expired or locked, 2 a check could not be completed.
    """
    parser = argparse.ArgumentParser(description="Check database password expiry.")
    parser.add_argument("--dblist", type=Path, default=Path(__file__).parent / "config/dblist.lst",
                        help="Path to the file containing the list of databases.")
    parser.add_argument("--workers", type=int, default=5,
                        help="Number of parallel workers.")
    parser.add_argument("--threshold", type=int, default=5,
                        help="Days before expiry at which a password is reported as expiring soon.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level.")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    databases = get_db_list(args.dblist)
    if not databases:
        logger.error("No databases to check.")
        return 2

    engines_to_check = set()
    for db in databases:
        if ':' in db:
            engines_to_check.add(db.split(':', 1)[0].strip().lower())

    sql_dir = Path(__file__).parent / "sql"
    sql_queries = {}

    if 'oracle' in engines_to_check:
        # Thick mode is required for EPS wallet connections
        try:
            oracledb.init_oracle_client()
            sql_queries['oracle'] = load_sql_query(sql_dir / "orcl_expire_check.sql")
        except Exception as e:
            logger.error(f"Failed to initialize Oracle Client, Oracle entries will be skipped: {e}")
    if 'postgresql' in engines_to_check:
        sql_queries['postgresql'] = load_sql_query(sql_dir / "pg_expire_check.sql")

    if not any(sql_queries.values()):
        logger.error("No valid SQL queries could be loaded.")
        return 2

    logger.info(f"Starting expiry check for {len(databases)} databases with {args.workers} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(check_db_expiry, db, sql_queries, args.threshold) for db in databases]
        statuses = [future.result() for future in futures]

    logger.info("Expiry check completed.")

    if STATUS_ERROR in statuses:
        return 2
    if any(status != STATUS_OK for status in statuses):
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
