#!/bin/env python

""" reporting handles all aspects of the various reports
    that Table Differ runs both on the 'diff_table' but also
    on the two tables being compared. Due to the potential size
    of the two tables, reports done on those are kept to a minimum.
"""

# BUILT IN
import logging

# THIRD PARTY
from sqlalchemy import text
from sqlalchemy.exc import NoSuchTableError, OperationalError

from rich.prompt import Prompt
from rich.console import Console
from rich.table import Table

# PERSONAL


class Reports:
    """Types of simple reporting this needs to return:
    - lists of database connection information
    - lists of given/used variables
    - counts of rows in origin, comp, and diff, tables
    - counts of identical rows
    - counts of modified rows
        - columns listed by most changed
        - how many rows in each column were changed
    """

    def __init__(self, conn, args):
        self.conn = conn
        self.args = args
        self.console = Console()  # rich text output formatting
        self.arg_table = None
        self.report_table = None
        self.reports = {
            "tables": {
                },
            "diff_table": {
                },
            }
        self.report()

    def report(self):
        self.counts()
        self.build_tables()
        dicts = self.args, self.reports
        tables = self.arg_table, self.report_table
        for i, d in enumerate(dicts):
            self.populate_tables(d, tables[i])
            self.console.print(tables[i])
        self.print_tables()


    def counts(self):
        def count_rows():
            query = self.conn.execute(
                text(f"""SELECT COUNT(*) FROM {self.args["table_info"]['table_initial']}""")
            )
            for result in query:
                self.reports["tables"][f"{self.args['table_info']['table_initial']} total rows"] = result[0]

        def count_same_rows():
            comp_string = ""
            for num, col in enumerate(self.args["table_info"]["comp_columns"]):
                if num == 0:
                    comp_string += f"A.{col} = B.{col}"
                else:
                    comp_string += f" AND A.{col} = B.{col}"

            query = self.conn.execute(
                text(
                    f"""
                SELECT COUNT(*)
                FROM (
                    SELECT *
                    FROM {self.args["table_info"]['table_initial']} A
                        INNER JOIN {self.args["table_info"]['table_secondary']} B
                        ON {comp_string})
                    """
                )
            )
            for result in query:
                self.reports["tables"]["same rows between both tables"] = result[0]

        def count_modified_rows():
            query = self.conn.execute(
                text(
                    f"""
                SELECT COUNT(*)
                FROM (
                    SELECT *
                    FROM {self.args["table_info"]['table_initial']}
                    EXCEPT
                    SELECT *
                    FROM {self.args["table_info"]['table_secondary']}
                    )
                """
                )
            )
            for count in query:
                self.reports["diff_table"]["modified rows between both tables"] = count[0]

        try:
            count_rows()
            count_same_rows()
            count_modified_rows()

        except OperationalError as e:
            logging.critical(f"[bold red blink]OPERATIONAL ERROR: [/] {e}")
        except NoSuchTableError as e:
            logging.critical(f"[bold red blink] NO SUCH TABLE ERROR: [/] {e}")
        finally:
            logging.debug(f"[bold red] REPORTS GATHERED: [/]{self.reports}")
            self.conn.commit()

    def build_tables(self):
        arg_table = Table(title="Arguments", caption="arguments used in Table Differ",)
        arg_table.add_column("sub dictionary key", style="red", no_wrap=True)
        arg_table.add_column("key", style="magenta", no_wrap=True)
        arg_table.add_column("value", style="cyan", no_wrap=True)

        report_table = Table(title="Reports", caption="Reports gathered",)
        report_table.add_column("report focus", style="red", no_wrap=True)
        report_table.add_column("report", style="magenta", no_wrap=True)
        report_table.add_column("result", style="cyan", no_wrap=True)

        self.arg_table = arg_table
        self.report_table = report_table

    def populate_tables(self, dictionary, table):
        sub_dict_keys = list(dictionary)
        for sub_dict in sub_dict_keys:
            keys = list(dictionary[sub_dict])
            for num, i in enumerate(keys):
                value = list(dictionary[sub_dict].values())
                if value[num] == value[0]:
                    table.add_row(f"{sub_dict}", f"{i}", f"{value[num]}")
                elif value[num] == value[-1]:
                    table.add_row("", f"{i}", f"{value[num]}", end_section=True)
                else:
                    table.add_row("", f"{i}", f"{value[num]}")

    def print_tables(self):
        """ Outputs all reports gathered onto the CLI using Rich Tables to
            improve readability.
        """
        def raw_tables():
            for table in self.args["table_info"]["tables"]:
                try:
                    query = self.conn.execute(
                        text(
                            f"""
                                SELECT * FROM {table}
                                """
                        )
                    )
                    raw_table = Table(title=f"{table}")
                    if table == self.args["table_info"]["tables"][-1]:
                        for col in self.args["table_info"]["diff_table_schema"]:
                            raw_table.add_column(f"{col}_{table}", style="cyan", no_wrap=True)
                    else:
                        for col in self.args["table_info"]["table_schema"]:
                            raw_table.add_column(f"{col}_{table}", style="cyan", no_wrap=True)

                    for result in query:
                        raw_table.add_row(*map(str, result))

                except OperationalError as e:
                    logging.critical(e)
                except OperationalError as e:
                    logging.critical(e)
                finally:
                    self.console.print(raw_table)

        if self.args["system"]["print_tables"] == "y":
            if Prompt.ask("[bold red]are you sure you want to print tables to the console? (y/n)")== "y":
                raw_tables()
