#! usr/bin/env python

""" create_diff_table handles all the aspects used in the creation of a 'diff_table'.
    This includes:
        - queries to each specific database
        - pieces of syntax correct SQL that each query may need such as:
                - arguments within the SELECT clause
                - arguments that are used to join the tables by
                - arguments that are used to exclude specific rows
        - function that stores the schema of actively used tables
            for usage within the queries
"""

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
        self.tables = ["A", "B"]

    def _select_args_universal(self):
        """This pieces together the initial SELECT arguments for the __diff_table__ creation
        using the comparison or ignore columns given.
        """
        string = ""
        for key in self.args["table_info"]["key_columns"]:
            string += f"A.{key} {key}, "

        if self.args["system"]["column_type"] == "comp":
            for table in self.tables:
                for col in self.args["table_info"]["comp_columns"]:
                    if (
                        table == "B"
                        and col == self.args["table_info"]["comp_columns"][-1]
                    ):
                        string += f'{table}.{col} {self.args["table_info"]["secondary_table_alias"]}_{col}'
                    else:
                        if table == "A":
                            string += f'{table}.{col} {self.args["table_info"]["initial_table_alias"]}_{col}, '
                        else:
                            string += f'{table}.{col} {self.args["table_info"]["secondary_table_alias"]}_{col}, '
            return string
        else:
            # this is the section that covers the ignored cols
            for table in self.tables:
                for col in self.args["table_info"]["table_cols"]:
                    if col not in self.args["table_info"]["ignore_columns"]:
                        print(
                            self.args["table_info"]["table_cols"][-1],
                            "ignore cols clause",
                        )
                        if (
                            table == "B"
                            and col == self.args["table_info"]["table_cols"][-1]
                        ):
                            string += f'{table}.{col} {self.args["table_info"]["secondary_table_alias"]}_{col}'
                        else:
                            if table == "A":
                                string += f'{table}.{col} {self.args["table_info"]["initial_table_alias"]}_{col}, '
                            else:
                                string += f'{table}.{col} {self.args["table_info"]["secondary_table_alias"]}_{col}, '
            return string

    def _key_join_universal(self):
        key_string = ""

        for key in self.args["table_info"]["key_columns"]:  # id, name
            if len(self.args["table_info"]["key_columns"]) == 0:
                key_string += f"A.{key} = B.{key}"
            else:
                if key == self.args["table_info"]["key_columns"][-1]:
                    key_string += f"A.{key} = B.{key}"
                else:
                    key_string += f"A.{key} = B.{key} AND "
        return key_string

    def _except_rows_universal(self):
        if self.args["table_info"]["except_rows"] is None:
            return ""
        string = """WHERE """
        for table in self.tables:
            for key in self.args["table_info"]["key_columns"]:  # id, name
                string += f"""{table}.{key} NOT IN ("""
                for row in self.args["table_info"]["except_rows"]:  # 2, 5
                    if row == self.args["table_info"]["except_rows"][-1]:
                        string += f"""{row}"""
                    else:
                        string += f"""{row}, """

                if key == self.args["table_info"]["key_columns"][-1]:
                    if table == self.tables[-1]:
                        string += """)"""
                    else:
                        string += """) AND """
                else:
                    string += """) AND """
        return string


class GetSchemas:
    def __init__(self, args, conn):
        self.args = args
        self.conn = conn
        self._get_schema()

    def _get_schema(self):
        """This returns the column names within the two tables to be used in several parts of
        table_differ. The most important usage is within the sql builder functions where the
        names of columns are integral in order to properly piece together the arguments.
        Active issues:
            - there is copied code that may be used in every different
                database version of the queries due to how different dbs
                handle accessing a schema (specifically a schema that is not known)


        """
        db = self.args["database"]["db_type"]
        if db == "postgres":
            cur = self.conn.cursor()
            table = []
            diff = []

            for table in (self.args["table_info"]["table_initial"], self.args["table_info"]["table_diff"]):
                col_names = f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = {self.args["table_info"]["schema_name"]}
                        AND
                        table_name = {table}
                    """
                cur.execute(col_names)
                columns = cur.fetchall()
                for result in columns:
                    if table == table[0]:
                        table.append(result)
                    else:
                        diff.append(result)

        elif db == "sqlite":
            table_schema = self.conn.execute(
                text(
                    f"""PRAGMA table_info({self.args["table_info"]['table_initial']})"""
                )
            )

            diff_table_schema = self.conn.execute(
                text(f"""PRAGMA table_info({self.args["table_info"]['table_diff']})""")
            )
            table = []
            diff = []
            for result in table_schema:
                table.append(result[1])
            for result in diff_table_schema:
                diff.append(result[1])

        elif db == "duckdb":
            raise NotImplementedError("duckdb not supported yet")
        elif db == "mysql":
            raise NotImplementedError("mysql not supported yet")

        try:
            for col in self.args["table_info"]["ignore_columns"]:
                table.remove(col)
        except TypeError:
            for col in table:
                if col not in self.args["table_info"]["comp_columns"]:
                    table.remove(col)

        self.args["table_info"]["table_cols"] = table
        self.args["table_info"]["diff_table_cols"] = diff


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
        GetSchemas(args, conn)
        self.pieces = QueryPieces(args, conn)

        ###########################
        # cursor object used in temp psycopg2 connection
        self.cur = conn.cursor()
        ###########################

    def create_diff_table(self):
        select_args = self.pieces._select_args_universal()
        key_join = self.pieces._key_join_universal()
        except_rows = self.pieces._except_rows_universal()

        drop_diff_table = (
            f"""DROP TABLE IF EXISTS {self.args["table_info"]["diff_table"]}"""
        )

        if self.args["database"]["db_type"] == "postgres":
            query = f"""
                CREATE TABLE {self.args["table_info"]["diff_table"]} AS
                SELECT
                {select_args}
                FROM {self.args['table_initial']} A
                    FULL OUTER JOIN {self.args['table_secondary']} B
                        ON {key_join}
                    {except_rows}
                """

        elif self.args["database"]["db_type"] == "sqlite":
            query = f"""
                CREATE TABLE IF NOT EXISTS {self.args["table_info"]["diff_table"]} AS
                    SELECT
                    {select_args}
                    FROM {self.args["table_info"]['table_initial']} A
                        INNER JOIN {self.args["table_info"]['table_secondary']} B
                            ON {key_join}
                        {except_rows}

                    UNION ALL
                    SELECT
                    {select_args}
                    FROM {self.args['table_info']['table_secondary']} B
                        LEFT OUTER JOIN {self.args['table_info']['table_initial']} A
                            ON {key_join}
                        WHERE A.{self.args['table_info']['key_columns'][0]} IS NULL

                    UNION ALL
                    SELECT
                    {select_args}
                    FROM {self.args['table_info']['table_secondary']} B
                        INNER JOIN {self.args['table_info']['table_initial']} A
                            ON {key_join}
                        WHERE A.{self.args['table_info']['key_columns'][0]} IS NULL
                    """

        elif self.args["database"]["db_type"] == "duckdb":
            raise NotImplementedError("duckdb not supported yet")
        elif self.args["database"]["db_type"] == "mysql":
            raise NotImplementedError("mysql not supported yet")

        try:
            logging.debug(f"[bold red]Diff Query[/]: {query}")
            logging.debug(f"[bold red]Select Arg[/]: {select_args}")
            logging.debug(f"[bold red]Key Join[/]: {key_join}")
            logging.debug(f"[bold red]Except Rows[/]: {except_rows}")

            ####################################
            # additional if else block added for psycopg2 cursor object
            if self.args["database"]["db_type"] == "postgres":
                self.cur.execute(drop_diff_table)
                self.cur.execute(query)
            else:
                self.conn.execute(text(drop_diff_table))
                self.conn.execute(text(query))
            ####################################

            GetSchemas(self.args, self.conn)
        except OperationalError as e:
            logging.critical(f"[bold red blink]OPERATIONAL ERROR:[/] {e}")

        except NoSuchTableError as e:
            logging.critical(f"[bold red blink] NO TABLE ERROR:[/] {e}")

        finally:
            self.conn.commit()
