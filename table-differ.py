#! /usr/bin/env python3

# long-help
""" Table Differ: Table comparison utility to be used within a SQLite, PostgreSQL, MySQL, or DuckDB database.
    Table comparison is achieved by creating a 'diff_table' out of changes between two
    different related tables based on given keys and what columns to focus on.
    Table Differ then conducts various reports on the contents of the 'diff_table'
    and prints them to the CLI.

    Table Differ picks up changes between the two tables as:
        - any row that has changed values within selected columns
        - any row that is missing information that exists in the other table
        - any row that exists within one table but not within the other

    Arguments that Table-Differ accepts:
    # REQUIRED ARGUMENTS
        -c --comparison_columns     specified columns to focus on
        -i --ignore_columns         specified columns to ignore

        note that while each argument can accept n number of values,
        only one of the two can be used on any single run.

    # SEMI-OPTIONAL ARGUMENTS
        --configs                   a value of 'y' will signal to table differ to
                                    source the next 3 values within a configs.yaml file
                                    instead of from CLI arguments

        -d --db_type                   specifies what type of database to connect to

        -t --tables                 requires 3 arguments that are the names of the
                                    initial and secondary table to be used in table-differ
                                    along with the name that would like to use for the 
                                    diff_table that will be created.

        -k --key_columns            specifies the name or names of key columns used
                                    to connect the two tables by

    # OPTIONAL ARGUMENTS
        -e --except_rows               signals Table Differ to ignore specific rows within
                                    each table based on the value of their key column(s)

        -l --logging_level          sets the logging level of Table Differ (default CRITICAL)

        -p --print_tables           attempts to print both of the tables used in the comparison
                                    to the CLI. This is only to be used with small tables and will
                                    certainly cause issues when applied to very large tables
"""
# example testing run
""" ./table-differ.py -i info --configs y -p y
"""

# BUILT-INS
import logging
from os.path import expanduser

# THIRD PARTY
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine

# PERSONAL
from modules import get_args
from modules.create_diff_table import Tables
from modules.reporting import Reports


def main():
    args = get_args.get_args()
    conn = create_connection(
        args
    )  # creates the connection to database using sqlalchemy

    tables = Tables(args, conn)
    tables.create_diff_table()  # generates initial diff_table
    Reports(conn, args)  # generates simple reporting


def create_connection(args):
    """Attempts to connect to database using SQLAlchemy and a URL that is pieced together from
    components in config.yaml
    """

    def create_url():
        # build into try accept block - links to documentation
        db_url = None
        try:
            if args["database"]["db_type"] == "postgres":
                with open(expanduser("~/.pgpass"), "r") as f:
                    host, port, database, user, password = f.read().split(":")
                db_url = "postgresql+pyscopg2://{}:{}@{}:{}/{}".format(
                    user, password, host, port, database
                )

            elif args["database"]["db_type"] == "mysql":
                with open(expanduser("~/.my.cnf"), "r") as f:
                    host, port, database, user, password = f.read().split(":")
                db_url = "mysql+pymysql://{}:{}@{}:{}/{}".format(
                    user, password, host, port, database
                )

            elif args["database"]["db_type"] == "sqlite":
                db_url = f'sqlite+pysqlite:///:{args["db_name"]}:'
            elif args["database"]["db_type"] == "duckdb":
                raise NotImplementedError("duckdb not supported yet")
        except OSError:
            print("could not open/read file", f)
            sys.exit()
        finally:
            logging.info(f"[bold red] DATABASE URL:[/] {db_url}")
            return db_url

    if args["system"]["local_db"] == "y":
        db_url = args["database"]["db_path"]
    else:
        db_url = create_url()

    try:
        engine = create_engine(db_url, echo=False, future=True)
        conn = engine.connect()
    except SQLAlchemyError as e:
        print(f"ERROR: {str(e)}")
    finally:
        logging.info(f"[bold red]CURRENT CONNECTION:[/]  {conn}")
        return conn


if __name__ == "__main__":
    main()
