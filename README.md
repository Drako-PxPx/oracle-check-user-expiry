> **Superseded (2026-09-29):** this tool is replaced by **dbcred-rotator**, which does the same
> Oracle/PostgreSQL expiry checking (`dbcred check`) plus password rotation and KeePass/EPS/pgpass
> sync. This repository is kept for history and is being archived. The fixes on the
> `fix/expiry-check-bugs` branch (PostgreSQL expiry query, expired/locked detection, exit codes)
> are carried over and validated in the successor.

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

# Report passwords expiring within 10 days
python check_expiry.py --threshold 10

# Adjust the number of parallel workers (default is 5)
python check_expiry.py --workers 10

# Combine arguments
python check_expiry.py --dblist config/prod_dblist.lst --workers 20
```

### Command Line Arguments

| Argument | Default | Description |
| :--- | :--- | :--- |
| `--dblist` | `config/dblist.lst` | Path to the file containing the list of database aliases. |
| `--threshold` | `5` | Days before expiry at which a password is reported as expiring soon. |
| `--workers` | `5` | Number of parallel threads to use for checking databases. |
| `--log-level` | `INFO` | Logging level displayed on screen, soon to be expired passwords are displayed on WARNING level, all connection errors are reported on ERROR level |

The queries live in `sql/orcl_expire_check.sql` (reads `USER_USERS`) and `sql/pg_expire_check.sql` (reads `pg_roles`), neither requires extra privileges.

### Exit codes

| Code | Meaning |
| :--- | :--- |
| `0` | All passwords nominal (or without expiry date). |
| `1` | At least one password is expiring soon, expired or the account is locked. |
| `2` | At least one database could not be checked (connection error, bad entry, missing SQL). |

An Oracle account whose password already expired can't log in, so it's reported from the `ORA-28001` login error. PostgreSQL reports an expired password as a plain authentication failure, which is indistinguishable from a wrong password and is reported as an error.
