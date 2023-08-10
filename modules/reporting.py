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
        self.reports = {}
        self._counts()

    def _counts(self):
        def count_rows():
            counts_origin = self.conn.execute(
                text(f"""SELECT COUNT(*) FROM {self.args['table_initial']}""")
            )
            for count in counts_origin:
                self.reports[f'rows in {self.args["table_initial"]}'] = f"{count[0]}"

            counts_comp = self.conn.execute(
                text(f"""SELECT COUNT(*) FROM {self.args['table_secondary']}""")
            )
            for count in counts_comp:
                self.reports[f'rows in {self.args["table_secondary"]}'] = f"{count[0]}"

            counts_diff = self.conn.execute(text("""SELECT COUNT(*) FROM diff_table"""))
            for count in counts_diff:
                self.reports["rows in diff_table"] = count[0]

        def count_same_rows():
            comp_string = ""
            for num, col in enumerate(self.args["comp_columns"]):
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
                    FROM {self.args['table_initial']} A
                        INNER JOIN {self.args['table_secondary']} B
                        ON {comp_string})
                    """
                )
            )
            for count in query:
                self.reports["same rows between both tables"] = count[0]

        def count_modified_rows():
            query = self.conn.execute(
                text(
                    f"""
                SELECT COUNT(*)
                FROM (
                    SELECT *
                    FROM {self.args['table_secondary']}
                    EXCEPT
                    SELECT *
                    FROM {self.args['table_initial']}
                    )
                """
                )
            )
            for count in query:
                self.reports["modified rows"] = count[0]

        def cols_by_changes():
            self.reports["rows with the most changes"] = "not yet implemented"

        try:
            count_rows()
            count_same_rows()
            count_modified_rows()
            cols_by_changes()

        except OperationalError as e:
            logging.critical(f"[bold red blink]OPERATIONAL ERROR: [/] {e}")
        except NoSuchTableError as e:
            logging.critical(f"[bold red blink] NO SUCH TABLE ERROR: [/] {e}")
        finally:
            self.conn.commit()
            logging.debug(
                f"[bold red] REPORTS GATHERED: [/]{self.reports}",
            )
            self.outputs()

    def outputs(self):
        """ Outputs all reports gathered onto the CLI using Rich Tables to
            improve readability.
        """
        def print_tables():
            self.args["tables"] = (
                self.args["table_initial"],
                self.args["table_secondary"],
                "diff_table",
            )
            for table in self.args["tables"]:
                try:
                    info = self.conn.execute(
                        text(
                            f"""
                                SELECT * FROM {table}
                                """
                        )
                    )
                    table_output = Table(title=f"{table}")
                    col_names = self.args["schema"]

                    if table == self.args["tables"][-1]:
                        schema = self.conn.execute(
                            text("""PRAGMA table_info(diff_table)""")
                        )
                        col_names = []
                        for row in schema:
                            col_names.append(row[1])

                    for col in col_names:
                        table_output.add_column(f"{col}", style="cyan", no_wrap=True)
                    for row in info:
                        table_output.add_row(*map(str, row))

                    self.console.print(table_output)
                except OperationalError as e:
                    logging.critical(e)
                except OperationalError as e:
                    logging.critical(e)

        if self.args["print_tables"] == "y":
            if (
                Prompt.ask(
                    "[bold red]are you sure you want to print tables to the console? (y/n)"
                )
                == "y"
            ):
                print_tables()

        report_table = Table(title="REPORTS")
        report_table.add_column("report", justify="right", style="cyan", no_wrap=True)
        report_table.add_column("result", style="magenta")

        values = list(self.args.values())
        for num, key in enumerate(self.args):
            report_table.add_row(f"{key}", f"{values[num]}")

        for num, report in enumerate(self.reports):
            report_table.add_row(
                f"{report}", f"{self.reports[report]}", style="Bold Red"
            )
        self.console.print(report_table)
