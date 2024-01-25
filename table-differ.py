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

        -t --tables                 requires 3 arguments that are the actual names of the
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

example testing run
./table-differ.py -i info --configs y -p y
"""

# BUILT-INS
import logging
from os.path import expanduser

# THIRD PARTY
import psycopg2

# PERSONAL
from modules import get_config
from modules.create_diff_table import DiffWriter
from modules.reporting import BasicReport


def main():
    args = get_config.get_config()
    db = args["database"]["db_type"]
    conn = create_connection(args, db)

    tables = DiffWriter(args, conn)
    tables.create_diff_table()  # generates initial diff_table
    basic_report = BasicReport(conn,
                                args['table_info']['schema_name'],
                                args['table_info']['table_initial'],
                                args['table_info']['table_secondary'],
                                args['table_info']['table_diff'],
                                args['table_info']['comp_cols'],
                                args['table_info']['ignore_cols'])
    basic_report.generate_report()


def create_connection(args, db: str):
    """Attempts to connect to database using SQLAlchemy and a URL that is pieced together from
    components in config.yaml
    """
    try:
        if db == "postgres":
            conn = psycopg2.connect(
                host=args["database"]["db_host"],
                database=args["database"]["db_name"],
                user=args["database"]["db_user"],
                port=args["database"]["db_port"],
            )
        elif db == "sqlite":
            assert args["database"]["db_path"]
            conn = sqlite3.connect(args["database"]["db_path"])
        else:
            raise ValueError(f'Invalid db value: {db}')
        logging.info(f"[bold red]CURRENT CONNECTION:[/]  {conn}")
    except Exception as e:
        print(f"ERROR: {str(e)}")
    finally:
        return conn


if __name__ == "__main__":
    main()
