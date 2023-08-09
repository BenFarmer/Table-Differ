#!/bin/env python

# BUILT-INS
import logging

# THIRD PARTY
from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy import text

FIRST_KEY = 0
SECOND_KEY = 1


class QueryPieces:
    """builds and returns the requested sql code to be inserted into the database dependent 'skeleton'
    code for building the diff table.
    Pieces that are required:
        - select arguments
        - row exceptions
        - key joins

    QueryPieces also gathers the names of each column within the table with self._get_schema()
    """

    def __init__(self, args, conn):
        self.args = args
        self.conn = conn
        self._get_schema()
        self.tables = ["A", "B"]

    def _select_args_universal(self):
        string = ""
        return string

    def _select_args_sqlite(self):
        """This pieces together the initial SELECT arguments for the __diff_table__ creation
        using the comparison or ignore columns given.
        """
        string = ""
        for key in self.args["key_columns"]:
            string += f"A.{key} {key}, "

        if self.args["column_type"] == "comp":
            for table in self.tables:
                for col in self.args["comp_columns"]:
                    if table == "B" and col == self.args["comp_columns"][-1]:
                        string += (
                            f'{table}.{col} {self.args["secondary_table_name"]}_{col}'
                        )
                    else:
                        if table == "A":
                            string += f'{table}.{col} {self.args["initial_table_name"]}_{col}, '
                        else:
                            string += f'{table}.{col} {self.args["secondary_table_name"]}_{col}, '
            return string
        else:
            for table in self.tables:
                for col in self.args["schema"]:
                    if col not in self.args["ignore_columns"]:
                        if table == "B" and col == self.args["schema"][-1]:
                            string += f'{table}.{col} {self.args["secondary_table_name"]}_{col}'
                        else:
                            if table == "A":
                                string += f'{table}.{col} {self.args["initial_table_name"]}_{col}, '
                            else:
                                string += f'{table}.{col} {self.args["secondary_table_name"]}_{col}, '
            return string

    def _key_join_universal(self):
        key_string = ""

        for key in self.args["key_columns"]:  # id, name
            if len(self.args["key_columns"]) == 0:
                key_string += f"A.{key} = B.{key}"
            else:
                if key == self.args["key_columns"][-1]:
                    key_string += f"A.{key} = B.{key}"
                else:
                    key_string += f"A.{key} = B.{key} AND "
        return key_string

    def _except_rows_universal(self):
        if self.args["except_rows"] is None:
            return ""
        string = """WHERE """
        for table in self.tables:
            for key in self.args["key_columns"]:  # id, name
                string += f"""{table}.{key} NOT IN ("""
                for row in self.args["except_rows"]:  # 2, 5
                    if row == self.args["except_rows"][-1]:
                        string += f"""{row}"""
                    else:
                        string += f"""{row}, """

                if key == self.args["key_columns"][-1]:
                    if table == self.tables[-1]:
                        string += """)"""
                    else:
                        string += """) AND """
                else:
                    string += """) AND """
        return string

    def _get_schema(self):
        """This returns the column names within the two tables to be used in several parts of
        table_differ. The most important usage is within the sql builder functions where the
        names of columns are integral in order to properly piece together the arguments.
        """
        db = self.args["db_type"]
        if db == "postgess":
            raise NotImplementedError("postgres not supported yet")
        elif db == "sqlite":
            schema = self.conn.execute(
                text(f"""PRAGMA table_info({self.args['table_initial']})""")
            )
        elif db == "duckdb":
            raise NotImplementedError("duckdb not supported yet")
        elif db == "mysql":
            raise NotImplementedError("mysql not supported yet")

        col_names = []
        for row in schema:
            col_names.append(row[1])
        self.args["schema"] = col_names


class Tables:
    """Tables controls the actual creation of the __diff_table__ based on
    what type of database the connection is secured with. Unfortunately the
    queries will have to be different depending on each databases unique
    quirks (ex.: sqlite not supporting FULL OUTER JOIN)
    """

    def __init__(self, args, conn):
        self.args = args
        self.conn = conn
        self.tables = ["A", "B"]

    def create_diff_table(self):
        pieces = QueryPieces(self.args, self.conn)

        select_args = pieces._select_args_universal()
        key_join = pieces._key_join_universal()
        except_rows = pieces._except_rows_universal()
        drop_diff_table = """DROP TABLE IF EXISTS diff_table"""

        if self.args["db_type"] == "postgres":
            query = f"""
                CREATE TABLE diff_table AS
                SELECT
                {select_args}
                FROM {self.args['table_initial']} A
                    FULL OUTER JOIN {self.args['table_secondary']} B
                        ON {key_join}
                    {except_rows}
                """
        elif self.args["db_type"] == "mysql":
            raise NotImplementedError("mysql not supported yet")
        elif self.args["db_type"] == "sqlite":
            select_args = pieces._select_args_sqlite()
            query = f"""
                CREATE TABLE IF NOT EXISTS diff_table AS
                    SELECT
                    {select_args}
                    FROM {self.args['table_initial']} A
                        INNER JOIN {self.args['table_secondary']} B
                            ON {key_join}
                        {except_rows}

                    UNION ALL
                    SELECT
                    {select_args}
                    FROM {self.args['table_secondary']} B
                        LEFT OUTER JOIN {self.args['table_initial']} A
                            ON {key_join}
                        WHERE A.{self.args['key_columns'][0]} IS NULL

                    UNION ALL
                    SELECT
                    {select_args}
                    FROM {self.args['table_secondary']} B
                        INNER JOIN {self.args['table_initial']} A
                            ON {key_join}
                        WHERE A.{self.args['key_columns'][0]} IS NULL
                    """

        elif self.args["db_type"] == "duckdb":
            raise NotImplementedError("duckdb not supported yet")

        try:
            logging.debug(f"[bold red]Diff Query[/]: {query}")
            logging.debug(f"[bold red]Select Arg[/]: {select_args}")
            logging.debug(f'[bold red]Schema[/]: {self.args["schema"]}')
            logging.debug(f"[bold red]Key Join[/]: {key_join}")
            logging.debug(f"[bold red]Except Rows[/]: {except_rows}")

            self.conn.execute(text(drop_diff_table))
            self.conn.execute(text(query))
        except OperationalError as e:
            logging.critical(f"[bold red blink]OPERATIONAL ERROR:[/] {e}")

        except NoSuchTableError as e:
            logging.critical(f"[bold red blink] NO TABLE ERROR:[/] {e}")

        finally:
            self.conn.commit()
