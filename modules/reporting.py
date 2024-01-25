#! usr/bin/env python

""" reporting handles all aspects of the various reports
    that Table Differ runs both on the 'diff_table' but also
    on the two tables being compared. Due to the potential size
    of the two tables, reports done on those are kept to a minimum.
"""

import logging

from rich.prompt import Prompt
from rich.console import Console
from rich.table import Table


class BasicReport:
        """Types of simple reporting this needs to return:
        - counts of rows in origin, comp, and diff, tables
        - counts of identical rows
        - counts of modified rows
        """

    def __init__(self,
                conn,
                schema_name: str,
                table_initial: str,
                table_secondary: str,
                table_diff: str,
                compare_cols: list[str],
                ignore_cols: list[str]):

        self.conn = conn
        self.schema_name = schema_name
        self.table_initial = table_initial
        self.table_secondary = table_secondary
        self.table_diff = table_diff
        self.compare_cols = compare_cols
        self.ignore_cols = ignore_cols


        def generate_report(self):
            self.get_counts()
            self.write_report()


        def get_counts(self):
            def count_rows():
                query = f"""SELECT COUNT(*) FROM {self.schema_name}.{self.table_initial}"""
                cur = self.conn.cursor()
                cur.execute(query)
                self.initial_table_row_cnt = cur.fetchall()[0][0]

        def count_same_rows():
            comp_string = ""
            for num, col in enumerate(self.compare_cols):
                if num == 0:
                    comp_string += f"A.{col} = B.{col}"
                else:
                    comp_string += f" AND A.{col} = B.{col}"

            query = f""" SELECT COUNT(*)
                         FROM (SELECT *
                            FROM {self.schema_name}.{self.table_initial} A
                           INNER JOIN {self.schema_name}.{self.table_secondary} B
                              ON {comp_string})
                          """
            cur = self.conn.cursor()
            cur.execute(query)
            self.row_match_cnt = cur.fetchall()[0][0]

    def count_modified_rows():
        query = f""" SELECT COUNT(*)
                     FROM (SELECT *
                            FROM {self.schema_name}.{self.table_initial}
                            EXCEPT
                            SELECT *
                                FROM {self.schema_name}.{self.table_secondary})
                    """
        cur = self.conn.cursor()
        cur.execute(query)
        self.row_diff_cnt = cur.fetchall()[0][0]

    count_rows()
    count_same_rows()
    count_modified_rows()
    self.conn.commit()


def write_report(self):
    report_table = Table(title="Basic Report")
    report_table.add_column("report", style="red", no_wrap=True)
    report_table.add_column("measure", style="magenta", no_wrap=True)
    report_table.add_column("result", style="cyan", no_wrap=True)
    report_table.add_row('Diff Table Build', 'initial table row count', str(self.initial_table_row_cnt))
    report_table.add_row('Diff Table Build', 'row match count', str(self.row_match_cnt))
    report_table.add_row('Diff Table Build', 'row diff count', str(self.row_diff_cnt))
    console = Console()    # rich text output formatting for CLI tables
    console.print(report_table)
