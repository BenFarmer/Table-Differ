#! /usr/bin/env python3

# BUILT-INS
import logging
import argparse
import yaml

# THIRD PARTY
from rich import print as rprint
from rich.logging import RichHandler


def get_args():
    """This builds the primary dictionary of arguments used through Table Differ,
    and also sets the logging level to be used while running. The arguments
    gathered come from passed in args through argparse and a config.yaml file
    """
    parser = argparse.ArgumentParser(description="table comparison utility")

    # REQUIRED ARGUMENTS
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-c",
        "--comparison-columns",
        nargs="+",
        action="store",
        default=[],
        help="columns to be compared, mutually exclusive with ignore_columns",
    )
    group.add_argument(
        "-i",
        "--ignore-columns",
        nargs="+",
        action="store",
        default=[],
        help="columns to not track/ignore, mutually exclusive with comparison_columns",
    )

    # SEMI-REQUIRED ARGUMENTS
    parser.add_argument(
        "--configs",
        action="store_true",
        default=None,
        help="would you like to use the additional variables stored in cfg",
    )
    parser.add_argument(
        "-d",
        "--db-type",
        choices=["postgres", "mysql", "sqlite", "duckdb"],
        help="database type",
    )
    parser.add_argument(
        "-t",
        "--tables",
        nargs=3,
        type=str,
        help="names of the 2 tables used in table-differ followed by the name for the diff_table",
    )
    parser.add_argument(
        "-k", "--key-columns", nargs="+", action="store", help="key columns to track"
    )

    # OPTIONAL ARGUMENTS
    parser.add_argument(
        "-e",
        "--ex-rows",
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
        "--print-tables",
        action="store_true",
        default=None,
        help="prints the tables to the console, use at your own risk",
    )
    parser.add_argument(
        "--local-db",
        action="store_true",
        default=None,
        help="designates whether or not to use a local sourced database",
    )

    def get_yaml():
        """Critical information is stored within the config.yaml file
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
        if args.configs is True:
            db_type = yaml_config["db_type"]
            table_initial = yaml_config["table_initial"]
            table_secondary = yaml_config["table_secondary"]
            table_diff = yaml_config["diff_table"]
            key_columns = yaml_config["key_columns"]
        else:
            db_type = args.db_type
            table_initial = args.tables[0]
            table_secondary = args.tables[1]
            table_diff = args.tables[2]
            key_columns = args.key_columns

        column_type = "comp"
        if len(args.comparison_columns) == 0:
            column_type = "ignore"

        if args.local_db is True:
            db_path = yaml_config["db_path"]
        else:
            db_path = None

        arg_dict = {
            "database": {
                "db_host": yaml_config["db_host"],
                "db_port": yaml_config["db_port"],
                "db_name": yaml_config["db_name"],
                "db_user": yaml_config["db_user"],
                "db_type": db_type,
                "db_path": db_path,
            },
            "table_info": {
                "table_initial": table_initial,  # name of 1st table being queried
                "table_secondary": table_secondary,  # name of 2nd table being queried
                "diff_table": table_diff,

                "schema_name": yaml_config["schema_name"],
                "table_cols": "null",  # name of the columns in the 2 tables queried
                "diff_table_cols": "null",  # name of the columns in the diff table
                "key_columns": key_columns,
                "comp_columns": args.comparison_columns,
                "ignore_columns": args.ignore_columns,
                "initial_table_alias": yaml_config[
                    "initial_table_alias"
                ],  # alias for 1st table
                "secondary_table_alias": yaml_config[
                    "secondary_table_alias"
                ],  # alias for 2nd table
                "except_rows": args.ex_rows,
            },
            "system": {
                "local_db": args.local_db,
                "print_tables": args.print_tables,
                "column_type": column_type,
            },
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
