#!/bin/env python3

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

        -t --tables                 requires 2 arguments that are the names of the
                                    initial and secondary tables used in the comparison

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

# BUILT-INS
import logging
import argparse
import yaml
from os.path import expanduser

# THIRD PARTY
from rich import print as rprint
from rich.logging import RichHandler
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine

# PERSONAL
from modules.create_diff_table import Tables
from modules.reporting import Reports


def main():
    args = get_args()
    conn = create_connection(
        args
    )  # creates the connection to database using sqlalchemy

    tables = Tables(args, conn)
    tables.create_diff_table()  # generates initial diff_table
    Reports(conn, args)  # generates simple reporting


def get_args():
    """ This builds the primary dictionary of arguments used through Table Differ,
        and also sets the logging level to be used while running. The arguments
        gathered come from passed in args through argparse and a config.yaml file
    """
    parser = argparse.ArgumentParser(description="table comparison utility")

    # REQUIRED ARGUMENTS
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-c",
        "--comparison_columns",
        nargs="+",
        action="store",
        help="columns to be compared, mutually exclusive with ignore_columns",
    )
    group.add_argument(
        "-i",
        "--ignore_columns",
        nargs="+",
        action="store",
        help="columns to not track/ignore, mutually exclusive with comparison_columns",
    )

    # SEMI-REQUIRED ARGUMENTS
    parser.add_argument(
        "--configs",
        choices=["y", "n"],
        default=["n"],
        help="would you like to use the additional variables stored in cfg",
    )
    parser.add_argument(
        "-d",
        "--db_type",
        choices=["postgres", "mysql", "sqlite", "duckdb"],
        help="database type",
    )
    parser.add_argument(
        "-t",
        "--tables",
        nargs=2,
        type=str,
        help="two tables used in comparison, the first one will be refered to as the inital or origin table",
    )
    parser.add_argument(
        "-k", "--key_columns", nargs="+", action="store", help="key columns to track"
    )

    # OPTIONAL ARGUMENTS
    parser.add_argument(
        "-e",
        "--except_rows",
        nargs="+",
        action="store",
        help="potential rows to be excluded from diff_table. These should be key column values",
    )
    parser.add_argument(
        "-l",
        "--logging_level",
        default="warning",
        choices=["debug", "info", "warning", "error", "critical"],
        help="logging output level: debug, info, warning, error, critical",
    )
    parser.add_argument(
        "-p",
        "--print_tables",
        choices=["y", "n"],
        default=["n"],
        help="prints the tables to the console, use at your own risk",
    )
    parser.add_argument(
        "--local_db",
        choices=["y", 'n'],
        default=['n'],
        help="designates whether or not to use a local sourced database"
    )

    def get_yaml():
        """ Critical information is stored within the config.yaml file
            and this retrieves it and stores it in an accessable way
            as 'yaml_config'
        """
        with open("config.yaml") as stream:
            try:
                yaml_config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                logging.critical(exc)
            finally:
                return yaml_config

    def build_arg_dict(yaml_config):
        if args.configs == "y":
            db_type = yaml_config["db_type"]
            table_initial = yaml_config["table_initial"]
            table_secondary = yaml_config["table_secondary"]
            key_columns = yaml_config["key_columns"]
            tables = [yaml_config["table_initial"], yaml_config["table_secondary"]]
        else:
            db_type = args.db_type
            table_initial = args.tables[0]
            table_secondary = args.tables[1]
            key_columns = args.key_columns
            tables = args.tables

        column_type = "comp"
        if args.comparison_columns is None:
            column_type = "ignore"

        arg_dict = {
            "db_host": yaml_config["db_host"],
            "db_port": yaml_config["db_port"],
            "db_name": yaml_config["db_name"],
            "db_user": yaml_config["db_user"],
            "db_type": db_type,
            "tables": tables,
            "table_initial": table_initial,
            "table_secondary": table_secondary,
            "key_columns": key_columns,
            "comp_columns": args.comparison_columns,
            "ignore_columns": args.ignore_columns,
            "personal_schema": yaml_config["personal_schema"],
            "initial_table_name": yaml_config["initial_table_name"],
            "secondary_table_name": yaml_config["secondary_table_name"],
            "print_tables": args.print_tables,
            "column_type": column_type,
            "except_rows": args.except_rows,
        }
        logging.info(f"[bold red]ARGUMENTS USED:[/]  {arg_dict}")
        return arg_dict

    args = parser.parse_args()
    setup_logging(args.logging_level)
    yaml_config = get_yaml()
    return build_arg_dict(yaml_config)


def setup_logging(logging_level):
    log_level = str(logging_level).upper()
    rprint(f"[bold red]Current Log Level:[bold red blink] {log_level}")

    FORMAT = "%(message)s"
    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format=FORMAT,
        datefmt="[%X]",
        handlers=[RichHandler(markup=True)],
    )


def create_connection(args):
    """ Attempts to connect to database using SQLAlchemy and a URL that is pieced together from
        components in config.yaml
    """
    def create_url():
        db_url = None
        if args["db_type"] == "postgres":
            with open(expanduser('~/.pgpass'), 'r') as f:
                host, port, database, user, password = f.read().split(':')
            db_url = 'postgresql+pyscopg2://{:{}@{}:{}/{}'.format(user, password, host, port, database)

        elif args["db_type"] == "mysql":
            db_url = f'mysql+pymysql://{args["db_user"]}:{password}@{args["db_host"]}:{args["db_port"]}/{args["db_name"]}'
        elif args["db_type"] == "sqlite":
            db_url = f'sqlite+pysqlite:///:{args["db_name"]}:'
        elif args["db_type"] == "duckdb":
            raise NotImplementedError("duckdb not supported yet")
        logging.info(
            f"[bold red]CONNECTION DETAILS:[/] db_host: {args['db_host']}, db_port: {args['db_port']}, db_name: {args['db_name']}, db_user: {args['db_user']}, db_url: {db_url}",
        )
        return db_url

    db_url = create_url()
    logging.info(f"[bold red] DATABASE URL:[/] {db_url}")
    try:
        engine = create_engine(db_url, echo=False, future=True)
        conn = engine.connect()
        logging.info(f"[bold red]CURRENT CONNECTION:[/]  {conn}")
        return conn
    except SQLAlchemyError as e:
        print(f"ERROR: {str(e)}")


if __name__ == "__main__":
    main()
