""" Returns a single consolidated dictionary of program configs
"""
# pylint: disable=line-too-long, logging-fstring-interpolation
# pylint: disable=missing-function-docstring

# BUILT-INS
import argparse
import logging
import sys
from typing import Optional
import yaml

# THIRD PARTY
from rich import print as rprint
from rich.logging import RichHandler



def get_config(cli_args=None):
    cli_args = get_cli_args(cli_args)
    setup_logging(cli_args.logging_level)
    yaml_config = get_yaml_config(cli_args.config_file)
    final_config = build_arg_dict(cli_args, yaml_config)
    return final_config



def get_cli_args(cli_args):
    """ Returns command line arguments
    """
    parser = argparse.ArgumentParser(description="table comparison utility")

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument( "-c", "--compare-cols",
                        nargs="+",
                        action="store",
                        default=[],
                        help="columns to be compared, mutually exclusive with ignore_columns")
    group.add_argument( "-i", "--ignore-cols",
                        nargs="+",
                        action="store",
                        default=[],
                        help="columns to not track/ignore, mutually exclusive with comparison_columns")

    parser.add_argument("--config-file",
                        help="name of config file")
    parser.add_argument( "-d", "--db-type",
                        choices=["postgres", "mysql", "sqlite", "duckdb"],
                        help="database type")
    parser.add_argument( "-k", "--key-cols",
                        nargs="+",
                        action="store",
                        help="key columns to track")

    # OPTIONAL ARGUMENTS
    parser.add_argument("-e", "--ex-rows",
                        nargs="+",
                        action="store",
                        help="potential rows to be excluded from diff_table. These should be key column values")
    parser.add_argument( "-l", "--logging_level",
                        default="warning",
                        choices=["debug", "info", "warning", "error", "critical"],
                        help="logging output level: debug, info, warning, error, critical")
    parser.add_argument("-p", "--print-tables",
                        action="store_true",
                        default=None,
                        help="prints the tables to the console, use at your own risk")
    parser.add_argument("--local-db",
                        action="store_true",
                        default=None,
                        help="designates whether or not to use a local sourced database")
    return parser.parse_args(cli_args)



def setup_logging(logging_level: str) -> None:
    log_level = str(logging_level).upper()
    rprint(f"[bold red]Current Log Level:[bold red blink] {log_level}")

    log_format = "%(message)s"
    logging.basicConfig(
        level=logging.getLevelName(log_level),
        format=log_format,
        datefmt="[%X]",
        handlers=[RichHandler(markup=True)])



def get_yaml_config(config_file: str) -> dict:
    """ Returns program config from file
    """
    with open(config_file, encoding='UTF-8') as stream:
        try:
            yaml_config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.critical(exc)
            if '.yml' not in config_file and '.yaml' not in config_file:
                logging.critical('Config_file does not appear to be a yaml file')
            raise
    return yaml_config



def build_arg_dict(args,
                   yaml_config: dict|None=None) -> dict:
    """ Returns consolidated config dictionary

    Arguments override config file items
    """
    if yaml_config is None:
        yaml_config = {}

    col_type = "comp"
    if not args.compare_cols and not yaml_config.get('compare_cols'):
        col_type = "ignore"

    def get_diff_table_name(diff_table: str,
                            secondary_table: str) -> str:
        if diff_table:
            return diff_table
        else:
            return secondary_table + '__diff__'


    if args.local_db:
        db_path = yaml_config["db_path"]
    else:
        db_path = None

    arg_dict = {
        "database": {
            "db_host": yaml_config["db_host"],
            "db_port": yaml_config["db_port"],
            "db_name": yaml_config["db_name"],
            "db_user": yaml_config["db_user"],
            "db_type": args.db_type or yaml_config.get('db_type'),
            "db_path": db_path },
        "table_info": {
            "table_initial": yaml_config['table_initial'],
            "table_secondary": yaml_config['table_secondary'],
            "table_diff": get_diff_table_name(yaml_config['diff_table'],
                                              yaml_config['table_secondary']),
            "schema_name": yaml_config["schema_name"],
            "table_cols": "null",  # name of the columns in the 2 tables queried
            "diff_table_cols": "null",  # name of the columns in the diff table
            "key_cols": args.key_cols or yaml_config.get('key_cols'),
            "comp_cols": args.compare_cols or yaml_config.get('compare_cols'),
            "ignore_cols": args.ignore_cols or yaml_config.get('ignore_cols'),
            "initial_table_alias": yaml_config["initial_table_alias"],  # alias for 1st table
            "secondary_table_alias": yaml_config["secondary_table_alias"],  # alias for 2nd table
            "except_rows": args.ex_rows},
        "system": {
            "local_db": args.local_db,
            "print_tables": args.print_tables,
            "col_type": col_type} }
    logging.info(f"[bold red]ARGUMENTS USED:[/]  {arg_dict}")
    return arg_dict
