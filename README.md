# Oracle Expiry Check Script

One of the common security practices is the password expiration for named users, if you have multiple accounts and there is not a federated account manager enabled for the database authentication this might be a problem.

For this problem I created this script which loops through all databases and reports if the logon user is going to expire soon (or if it expired already).

The `check_expiry.py` script loops through all TNS entries listed on `config/dblist.lst`, and report the user status on screen.

## Prerequisites

- **Python 3.x**
- **oracledb** library
- Oracle Client installed and configured (as this script uses EPS, this script is not compatible with thin library).
- Oracle EPS Wallet configured for passwordless connections for each TNS alias.

## How to Run

```bash
# Basic usage with default settings
python check_expiry.py

# Specify a custom database list file
python check_expiry.py --dblist /path/to/my/dblist.lst

# Specify a custom SQL file
python check_expiry.py --sql /path/to/custom_query.sql

# Adjust the number of parallel workers (default is 5)
python check_expiry.py --workers 10

# Combine arguments
python check_expiry.py --dblist config/prod_dblist.lst --workers 20
```

### Command Line Arguments

| Argument | Default | Description |
| :--- | :--- | :--- |
| `--dblist` | `config/dblist.lst` | Path to the file containing the list of database aliases. |
| `--sql` | `sql/expire_check.sql` | Path to the SQL file containing the expiry check query. |
| `--workers` | `5` | Number of parallel threads to use for checking databases. |
| `--log-level` | `INFO` | Logging level displayed on screen, soon to be expired passwords are displayed on WARNING level, all connection errors are reported on ERROR level |
