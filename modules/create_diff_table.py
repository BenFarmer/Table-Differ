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
from pprint import pprint as pp

# THIRD PARTY
from sqlalchemy.exc import NoSuchTableError, OperationalError
from sqlalchemy import text

FIRST_KEY = 0
SECOND_KEY = 1


class QueryClauses:
    """builds and returns the requested sql code to be inserted into the database dependent 'skeleton'
    code for building the diff table.
    """

    def __init__(self, 
                 table_cols: list[str],
                 key_cols: list[str],
                 compare_cols: list[str],
                 ignore_cols: list[str],
                 except_rows: list[int], #need to double check this is how rows would be passed
                 initial_table_alias: str,
                 secondary_table_alias: str):

        self.table_cols = table_cols
        self.key_cols = key_cols
        self.compare_cols = compare_cols
        self.ignore_cols = ignore_cols
        self.except_rows = except_rows
        self.initial_table_alias = initial_table_alias
        self.secondary_table_alias = secondary_table_alias

        if not compare_cols and not ignore_cols:
            raise ValueError('Must have either compare_cols or ignore_cols')

    def get_select(self) -> str:
        """This pieces together the initial SELECT arguments for the __diff_table__ creation
        using the comparison or ignore columns given.
        """
        string = ""
        for key in self.key_cols:
            string += f"    a.{key} {self.initial_table_alias}_{key}, \n"
            string += f"    a.{key} {self.secondary_table_alias}_{key}, \n"

        if self.compare_cols:
            usable_cols = sorted(list(set(self.compare_cols) - set(self.key_cols)))
        else:
            usable_cols = list(set(self.table_cols) - set(self.ignore_cols))
            usable_cols = sorted(list(set(usable_cols) - set(self.key_cols)))

        for col in usable_cols:
            string += f"    a.{col} {self.initial_table_alias}_{col}, \n"
            string += f"    a.{col} {self.secondary_table_alias}_{col}, \n"
        string = string.rstrip().rstrip(',')
        return string

    def get_join(self) -> str:
        string = ""
        return ' AND '.join([f' a.{x} = b.{x} ' for x in self.key_cols])

    def get_except(self) -> str:
        # this needs to be reformatted next
        if self.except_rows is None:
            return ""
        string = """WHERE """
        for table in self.tables:
            for key in self.key_columns:
                string += f"""{table}.{key} NOT IN ("""
                for row in self.except_rows:
                    if row == self.except_rows[-1]:
                        string += f"""{row}"""
                    else:
                        string += f"""{row}, """

#                if key == self.key_columns[-1]:
#                    if table == self.table_diff[-1]:
#                        string += """)"""
#                    else:
#                        string += """) AND """
#                else:
#                    string += """) AND """
        return string

# to be moved into separate module
######################################################################
def get_cols(conn,
             db_type: str,
             schema_name: str,
             table_name: str) -> list[str]:
    """This returns the column names within the two tables to be used in several parts of
    table_differ. The most important usage is within the sql builder functions where the
    names of columns are integral in order to properly piece together the arguments.
    """
    if db_type == "postgres":
        cur = conn.cursor()
        col_names = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}]
                AND table_name = '{table_name}'
        """
        cur.execute(col_names)
        columns = cur.fetchall()

    elif db_type == "sqlite":
        table_schema = conn.execute(
            text(f"""PRAGMA table_info({table_name})""")
        )
        columns = []
        for result in table_schema:
            columns.append(result[1])

    elif db_type == "duckdb":
        raise NotImplementedError("duckdb not supported yet")
    elif db_type == "mysql":
        raise NotImplementedError("mysql not supported yet")
    return columns

def get_common_cols(table_a_cols: list[str],
                    table_b_cols: list[str]) -> list[str]:
    return list(set(table_a_cols).intersection(set(table_b_cols)))
######################################################################

class DiffWriter:
    """Tables controls the actual creation of the __diff_table__ based on
    what type of database the connection is secured with. Unfortunately the
    queries will have to be different depending on each databases unique
    quirks (ex.: sqlite not supporting FULL OUTER JOIN)
    """

    def __init__(self, args, conn):
        self.args = args
        self.conn = conn
        self.tables = ["A", "B"]
        self.db_type = self.args["database"]["db_type"]
        self.table_initial = self.args["table_info"]["table_initial"]
        self.table_secondary = self.args["table_info"]["table_secondary"]
        self.table_diff = self.args["table_info"]["table_diff"]
        self.schema_name = self.args["table_info"]["schema_name"]
        self.key_cols = self.args["table_info"]["key_columns"]
        self.compare_cols = self.args["table_info"]["comp_columns"]
        self.ignore_cols = self.args["table_info"]["ignore_columns"]
        self.except_rows = self.args["table_info"]["except_rows"]
        self.initial_table_alias = self.args["table_info"]["initial_table_alias"]
        self.secondary_table_alias = self.args["table_info"]["secondary_table_alias"]


    def _get_clauses(self):
        initial_table_cols = get_cols(self.conn,
                                         self.db_type,
                                         self.schema_name,
                                         self.table_initial)
        secondary_table_cols = get_cols(self.conn,
                                         self.db_type,
                                         self.schema_name,
                                         self.table_secondary)
        common_table_cols = get_common_cols(initial_table_cols, secondary_table_cols)
        clauses = QueryClauses(
                table_cols = common_table_cols,
                key_cols = self.key_cols,
                compare_cols = self.compare_cols,
                ignore_cols = self.ignore_cols,
                except_rows = self.except_rows,
                initial_table_alias = self.initial_table_alias,
                secondary_table_alias = self.secondary_table_alias)
        return clauses

    def _assemble_create_query_psql(self):
        select_clause = clauses.get_select()
        join_clause = clauses.get_join()
        query = f"""
                CREATE TABLE {self.schema_name}.{self.table_diff} AS
                SELECT {select_clauses}
                FROM {self.schema_name}.{self.table_initial} A
                    FULL OUTER JOIN {self.schema_name}.{self.table_secondary} B
                    ON {join_clause}
                """
        return query

    def _assemble_drop_query(self):
        if self.db_type == 'postgres' or 'mysql':
            table_reference = f"{self.schema_name}.{self.table_diff}"
        else:
            table_reference = f"{self.table_diff}"
        drop_query = (
            f"""DROP TABLE IF EXISTS {table_reference}""")
        return drop_query

# this needs reformatting next, placing query frame here temporarily
    def _assemble_create_query_sqlite(self, clauses):
        select_clause = clauses.get_select()
        join_clause = clauses.get_join()
        except_clause = clauses.get_except()
        if self.args["database"]["db_type"] == "sqlite":
            query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_diff} AS
                    SELECT
                    {select_clause}
                    FROM {self.table_initial} A
                        INNER JOIN {self.table_secondary} B
                            ON {join_clause}
                        {except_clause}

                    UNION ALL
                    SELECT
                    {select_clause}
                    FROM {self.table_secondary} B
                        LEFT OUTER JOIN {self.table_initial} A
                            ON {join_clause}
                        WHERE A.{self.args['table_info']['key_columns'][0]} IS NULL

                    UNION ALL
                    SELECT
                    {select_clause}
                    FROM {self.table_secondary} B
                        INNER JOIN {self.table_initial} A
                            ON {join_clause}
                        WHERE A.{self.args['table_info']['key_columns'][0]} IS NULL
                    """
        return query

    def create_diff_table(self):
        clauses = self._get_clauses()

        if self.db_type == 'sqlite':
            create_query = self._assemble_create_query_sqlite(clauses)
            drop_query = self._assemble_drop_query()
        elif self.db_type == 'postgres':
            create_query = self._assemble_create_query_psql(clauses)
            drop_query = self._assemble_drop_query()
        else:
            raise NotImplementedError('sorry this db isnt implemented')

        logging.debug(f"[bold red] Diff Query[/]: {create_query}")

        try:
            if self.db_type == 'sqlite' or 'duckdb' or 'mysql':
                self.conn.execute(text(drop_query))
                self.conn.execute(text(create_query))
            elif self.db_type == 'postgres':
                cur = self.conn.cursor()
                cur.execute(drop_query)
                cur.execute(create_query)
        except OperationalError as e:
            logging.critical(f"[bold red blink]OPERATIONAL ERROR:[/] {e}")
        except NoSuchTableError as e:
            logging.critical(f"[bold red blink] NO TABLE ERROR:[/] {e}")

        finally:
            self.conn.commit()
            print('Diff Table Created')
